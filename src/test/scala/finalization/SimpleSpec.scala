package finalization

import cats.effect.Sync
import cats.syntax.all._
import finalization.util.NetworkRunner
import finalization.util.RunAll.runAll
import monix.eval.Task
import org.scalatest.prop.PropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SimpleSpec extends FlatSpec with PropertyChecks with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run network with complete dag" in {
    runAll { runner: NetworkRunner[Task] =>
      runCompleteDag(runner)
    }
  }

  it should "run network with two isolated partitions" in {
    runAll { runner: NetworkRunner[Task] =>
      runTwoIsolatedPartitions[Task](runner)
    }
  }

  it should "run network with two equal partitions and merge at the end" in {
    runAll { runner: NetworkRunner[Task] =>
      runTwoEqualPartitionsWithMerge[Task](runner)
    }
  }

  it should "run network with two partitions merge" in {
    runAll { runner: NetworkRunner[Task] =>
      runTwoPartitionsMerge[Task](runner)
    }
  }

  it should "run network with split on genesis" in {
    runAll { runner: NetworkRunner[Task] =>
      runGenesisSplit[Task](runner)
    }
  }

  def runCompleteDag[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(senders = 5)
    for {
      net1 <- runSections(net, List((6, .0f)))

      _ <- printDag(net1, "complete")
    } yield ()
  }

  def runTwoIsolatedPartitions[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    implicit val nm = runner.netMngr

    val net = genNetwork(senders = 4)

    for {
      net1 <- runSections(net, List((1, .0f)))

      // Split 4 on 2-2
      Vector(fst, snd) = net1.split(Seq(2, 2))

      fst1 <- runSections(fst, List((3, .0f)))

      _ <- printDag(fst1, "isolated/dag-1")

      snd1 <- runSections(snd, List((4, .0f)))

      _ <- printDag(snd1, "isolated/dag-2")

      net2 = fst1 >|< snd1

      _ <- printDag(net2, "isolated/dag-3")
    } yield ()
  }

  def runTwoEqualPartitionsWithMerge[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(senders = 4)

    for {
      net1 <- runSections(net, List((1, .0f)))

      // Split 4 on 2-2
      Vector(s1_1_net, s1_2_net) = net1.split(Seq(2, 2))

      d_s1_1 <- runSections(s1_1_net, List((2, .0f)))
      d_s1_2 <- runSections(s1_2_net, List((2, .0f)))

      net2                  = d_s1_1 >|< d_s1_2
      Vector(s2_1, s2_rest) = net2.split(Seq(1))

      // Run 1 witness of 2 partitions
      s2_1_end <- runSections(s2_1, List((1, .0f)))

      net3 = s2_1_end >|< s2_rest

      _ <- printDag(net3, "equal-partitions-merge")
    } yield ()
  }

  def runTwoPartitionsMerge[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(senders = 4)

    for {
      net1 <- runSections(net, List((1, .0f)))

      // Split 4 on 2-2
      Vector(s1_1, s1_2) = net1.split(Seq(2, 2))

      s1_1_end <- runSections(s1_1, List((3, .0f)))

      s1_2_end <- runSections(s1_2, List((1, .0f)))

      net2 = s1_1_end >|< s1_2_end

      net3 <- runSections(net2, List((2, .0f)))

      // Split 4 on 1-3
      Vector(s2_1, s2_2) = net3.split(Seq(1, 3))

      s2_2_end <- runSections(s2_2, List((1, .0f)))

      // Split 3 on 1-2
      Vector(s3_1_net, s3_2_net) = s2_2_end.split(Seq(1))

      s3_1_net_end <- runSections(s3_1_net, List((1, .0f)))

      net4 = s2_1 >|< s3_1_net_end >|< s3_2_net

      netRes <- runSections(net4, List((2, .0f)))

      _ <- printDag(netRes, "partitions-merge")
    } yield ()
  }

  def runGenesisSplit[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(senders = 5)

    // Split 6 on 3-3
    val Vector(s1_1, s1_2) = net.split(Seq(3, 2))

    for {
      s1_1_end <- runSections(s1_1, List((1, 0f)))

      s1_2_end <- runSections(s1_2, List((3, 0f)))

      net2 = s1_1_end >|< s1_2_end

      Vector(s2_1, s2_2, s2_rest) = net2.split(Seq(2, 1))

      s2_2_end <- runSections(s2_2, List((1, 0f)))

      net3 = s2_2_end >|< s2_1 >|< s2_rest

      Vector(s3_1, s3_2) = net3.split(Seq(4, 1))

      s3_2_end <- runSections(s3_2, List((1, 0f)))
      net4      = s3_1 >|< s3_2_end

      Vector(s4_1, s4_2, s4_rest) = net4.split(Seq(3, 1))
      s4_2_end                   <- runSections(s4_2, List((1, 0f)))

      netRes = s4_1 >|< s4_2_end >|< s4_rest

      _ <- printDag(netRes, "genesis-split-merge")
    } yield ()
  }

}
