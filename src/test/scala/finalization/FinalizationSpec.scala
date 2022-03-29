package finalization

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import finalization.Finalization._
import graphz._
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class FinalizationSpec extends FlatSpec with Matchers {

  class NetworkRunner[F[_]: Sync] {

    def printDag(network: Network, name: String) = {
      // DAG toposort
      val msgs = network.senders.head.heightMap
      val topo = msgs.map { case (_, v) => v.toVector }.toVector

      for {
        ref <- Ref[F].of(Vector[String]())
        _   <- {
          implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
          GraphGenerator.dagAsCluster[F](topo, "")
        }
        res <- ref.get
        _    = {
          val graphString = res.mkString

          val filePrefix = s"vdags/$name"

          // Save graph file
          //          Files.writeString(Path.of(s"$filePrefix.dot"), graphString)

          // Generate dot image
          import java.io.ByteArrayInputStream
          import scala.sys.process._

          val imgType  = "jpg"
          val fileName = s"$filePrefix.$imgType"
          println(s"Generating dot image: $fileName")

          val dotCmd = Seq("dot", s"-T$imgType", "-o", fileName)
          dotCmd #< new ByteArrayInputStream(graphString.getBytes) lineStream
        }
      } yield ()
    }

    def runSections(start: Network, roundSkip: List[(Int, Float)], prefix: String) = {
      val senderCount = start.senders.size
      roundSkip.foldM(start, 0) { case ((net, n), (height, skipP)) =>
        runNetwork(net, height, skipP).flatMap { res =>
          //            val name = s"$prefix-$n-$senderCount-$skipP"
          val name = s"$prefix-$n-$senderCount"

          printDag(res, name).as((res, n + 1))
        }
      }
    }

    def genNet(senders: Int) =
      initNetwork(sendersCount = senders, stake = 1) -> s"net$senders"

    def runDagComplete = {
      val (net, _) = genNet(6)
      runSections(net, List((6, .0f)), s"complete")
    }

    def runRandom = {

      val nets = List(6).map(genNet)

      nets.traverse { case (net, name) =>
        for {
          net1_    <- runSections(net, List((1, .0f)), s"main1-$name")
          //            net1_     <- runSections(net, List((1, .4f)), s"main1-$name")
          (net1, _) = net1_

          // Split network
          (fst, snd) = net1.split(.3f)

          _ = println(s"Split in ${fst.senders.size} and ${snd.senders.size}")

          //            fst1_ <- runSections(fst, List((7, .5f), (3, .3f)), s"fst-$name")
          fst1_    <- runSections(fst, List((5, .5f)), s"fst-$name")
          (fst1, _) = fst1_

          //            snd1_ <- runSections(snd, List((5, .4f), (5, .5f)), s"snd-$name")
          snd1_    <- runSections(snd, List((4, .4f)), s"snd-$name")
          (snd1, _) = snd1_

          // Merge networks
          net2 = fst1 >|< snd1
          _   <- runSections(net2, List((3, .0f)), s"main2-$name")
        } yield ()
      }
    }

    def runRandomWithLazy(maxRounds: Int) = {

      val initialNet = genNet(10)

      (initialNet, 0).tailRecM { case ((net, name), n) =>
        val run = for {
          net1_    <- runSections(net, List((1, 0f)), s"main1-$name")
          (net1, _) = net1_

          // Split network
          Vector(fst, snd) = net1.split(Seq(1))

          _ = println(s"Split in ${fst.senders.size} and ${snd.senders.size}")

          fst1_    <- runSections(fst, List((5, .5f)), s"fst-$name")
          (fst1, _) = fst1_

          snd1_    <- runSections(snd, List((4, .4f)), s"snd-$name")
          (snd1, _) = snd1_

          // Merge networks
          net2 = fst1 >|< snd1

          skip = if (n == 0) 0f else .3f

          net3_    <- runSections(net2, List((3, skip)), s"main2-$name")
          (net3, _) = net3_
        } yield (net3, s"round-$n")

        val recResult =
          if (n < maxRounds)
            run map (x => (x, n + 1).asLeft[Unit])
          else
            ().asRight[((Network, String), Int)].pure[F]

        recResult
      }
    }
  }

  implicit val s = monix.execution.Scheduler.global

  val sut = new NetworkRunner[Task]()

  it should "run network with complete dag" in {
    sut.runDagComplete.runSyncUnsafe()
  }

  it should "run random network" in {
    sut.runRandom.runSyncUnsafe()
  }

  it should "run random network with lazy sender" in {
    sut.runRandomWithLazy(5).runSyncUnsafe()
  }

}
