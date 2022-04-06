package finalization

import cats.effect.Sync
import cats.syntax.all._
import finalization.util.NetworkRunner
import finalization.util.RunAll.runAll
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class RandomLazySpec extends FlatSpec with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run random random network with lazy sender" in {
    runAll { runner: NetworkRunner[Task] =>
      runRandomWithLazy(maxRounds = 5, runner)
    }
  }

  def runRandomWithLazy[F[_]: Sync](maxRounds: Int, runner: NetworkRunner[F]) = {
    import runner._

    val initialNet = genNetwork(senders = 6)

    (initialNet, 0).tailRecM { case (net, n) =>
      val run = for {
        net1 <- runSections(net, List((1, 0f)))

        // Split network
        Vector(fst, snd) = net1.split(Seq(1))

        // Run both branches
        fst1 <- runSections(fst, List((5, .5f)))
        snd1 <- runSections(snd, List((4, .4f)))

        // Merge networks
        net2 = fst1 >|< snd1

        skip = if (n == 0) 0f else .3f

        net3 <- runSections(net2, List((3, skip)))

        _ <- printDag(net3, s"random-lazy/round-$n")
      } yield net3

      val recResult =
        if (n < maxRounds)
          run map (x => (x, n + 1).asLeft[Unit])
        else
          ().asRight[(Net, Int)].pure[F]

      recResult
    }
  }

}
