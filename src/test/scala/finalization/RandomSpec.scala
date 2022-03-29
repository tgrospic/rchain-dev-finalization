package finalization

import cats.effect.Sync
import cats.syntax.all._
import finalization.util.NetworkRunner
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class RandomSpec extends FlatSpec with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run random network" in {
    runRandom[Task].runSyncUnsafe()
  }

  def runRandom[F[_]: Sync] = {

    val runner = new NetworkRunner[F](enableOutput = true)

    import runner._

    val (net, name) = genNet(10)

    for {
      net1_    <- runSections(net, List((1, .0f)), s"start-$name")
      (net1, _) = net1_

      // Split network
      (fst, snd) = net1.split(.3f)

      fst1_    <- runSections(fst, List((5, .5f)), s"fst-$name")
      (fst1, _) = fst1_

      snd1_    <- runSections(snd, List((4, .4f)), s"snd-$name")
      (snd1, _) = snd1_

      // Merge networks
      net2 = fst1 >|< snd1

      (n11, n12)   = net2.split(.4f)
      (n111, n112) = n11.split(.5f)

      n111end_    <- runSections(n111, List((10, .5f)), s"n111-$name")
      (n111end, _) = n111end_
      n112end_    <- runSections(n112, List((4, .1f)), s"n112-$name")
      (n112end, _) = n112end_
      n12end_     <- runSections(n12, List((5, .4f)), s"n12-$name")
      (n12end, _)  = n12end_

      net3 = n112end >|< n12end
      net4 = net3 >|< n111end

      (n21, n22)   = net4.split(.3f)
      (n211, n212) = n21.split(.5f)

      n211end_    <- runSections(n211, List((13, .4f)), s"n211-$name")
      (n211end, _) = n211end_
      n212end_    <- runSections(n212, List((8, .4f)), s"n212-$name")
      (n212end, _) = n212end_
      n22end_     <- runSections(n22, List((5, .4f)), s"n22-$name")
      (n22end, _)  = n22end_

      net5 = n212end >|< n211end
      net6 = net5 >|< n22end

      r <- runSections(net6, List((5, .0f)), s"result-$name")
    } yield r
  }

}
