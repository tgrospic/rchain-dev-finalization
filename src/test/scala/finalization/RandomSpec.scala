package finalization

import cats.effect.Sync
import cats.syntax.all._
import finalization.util.NetworkRunner
import finalization.util.RunAll.runAll
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class RandomSpec extends FlatSpec with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run random network" in {
    runAll { runner: NetworkRunner[Task] =>
      runRandom(size = 6)(runner)
    }
  }

  def runRandom[F[_]: Sync](size: Int)(runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(size)

    for {
      net1 <- runSections(net, List((1, .0f)))

      // Split network
      (fst, snd) = net1.split(.3f)

      fst1 <- runSections(fst, List((5, .5f)))
      snd1 <- runSections(snd, List((4, .4f)))

      // Merge networks
      net2 = fst1 >|< snd1

      _ <- printDag(net2, s"random/dag-1")

      (n11, n12)   = net2.split(.4f)
      (n111, n112) = n11.split(.5f)

      n111end <- runSections(n111, List((10, .33f)))
      n112end <- runSections(n112, List((4, .1f)))
      n12end  <- runSections(n12, List((5, .4f)))

      net4 = n112end >|< n12end >|< n111end

      _ <- printDag(net4, s"random/dag-2")

      (n21, n22)   = net4.split(.3f)
      (n211, n212) = n21.split(.5f)

      n211end <- runSections(n211, List((13, .4f)))
      n212end <- runSections(n212, List((8, .4f)))
      n22end  <- runSections(n22, List((5, .4f)))

      net6 = n212end >|< n211end >|< n22end

      _ <- printDag(net6, s"random/dag-3")

      net7 <- runSections(net6, List((8, .0f)))

      _ <- printDag(net7, s"random/dag-4")
    } yield ()
  }

}
