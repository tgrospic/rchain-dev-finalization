import cats.effect.ExitCode
import cats.syntax.all._
import monix.eval._

object main extends TaskApp {
  def run(args: List[String]): Task[ExitCode] =
    ExitCode.Success.pure[Task]
}
