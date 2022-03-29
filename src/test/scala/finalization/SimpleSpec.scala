package finalization

import cats.effect.Sync
import finalization.util.NetworkRunner
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class SimpleSpec extends FlatSpec with Matchers {

  import monix.execution.Scheduler.Implicits.global

  it should "run network with complete dag" in {
    runDagComplete[Task].runSyncUnsafe()
  }

  def runDagComplete[F[_]: Sync] = {

    val runner = new NetworkRunner[F](enableOutput = true)

    import runner._

    val (net, _) = genNet(6)

    runSections(net, List((6, .0f)), s"complete")
  }

}
