package finalization.util

import cats.syntax.all._
import monix.eval.Task
import monix.execution.Scheduler

object RunAll {
  def runAll[A, AssertionTy](f: A => Task[AssertionTy])(implicit runners: Seq[A], s: Scheduler) =
    runners.toList.traverse_(f).runSyncUnsafe()
}
