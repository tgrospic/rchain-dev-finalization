package finalization

import cats.effect.Sync
import cats.syntax.all._
import finalization.Finalization.Network
import finalization.util.NetworkRunner
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class RandomLazySpec extends FlatSpec with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run random random network with lazy sender" in {
    runRandomWithLazy[Task](maxRounds = 5).runSyncUnsafe()
  }

  def runRandomWithLazy[F[_]: Sync](maxRounds: Int) = {

    val runner = new NetworkRunner[F](enableOutput = true)

    import runner._

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
