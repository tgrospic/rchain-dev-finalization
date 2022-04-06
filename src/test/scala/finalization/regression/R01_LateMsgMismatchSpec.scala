package finalization.regression

import cats.effect.Sync
import cats.syntax.all._
import finalization.util.NetworkRunner
import finalization.util.RunAll.runAll
import monix.eval.Task
import org.scalatest.{FlatSpec, Matchers}

class R01_LateMsgMismatchSpec extends FlatSpec with Matchers {
  import monix.execution.Scheduler.Implicits.global

  "finalization" should "not fail with late message from sender" in {
    runAll { runner: NetworkRunner[Task] =>
      regressionTest(runner)
    }
  }

  private def regressionTest[F[_]: Sync](runner: NetworkRunner[F]) = {
    import runner._

    val net = genNetwork(senders = 10)

    val (n1022813136, n1083092637) = net.split(0.5096376f)
    for {
      n_167755778                 <- runSections(n1022813136, List((1, 0.6574136f)))
      n_1407371484                <- runSections(n1083092637, List((4, 0.78288853f)))
      n1316169810                  = n_167755778 >|< n_1407371484
      (n_2131838046, n171242518)   = n1316169810.split(0.24446732f)
      n1709027370                 <- runSections(n_2131838046, List((1, 0.35403788f)))
      n_936033305                 <- runSections(n171242518, List((8, 0.8514173f)))
      n_801110001                  = n1709027370 >|< n_936033305
      (n_1753456045, n_1181790855) = n_801110001.split(0.68316025f)
      n_1885490992                <- runSections(n_1753456045, List((12, 0.80235255f)))
      n_1112360861                <- runSections(n_1181790855, List((10, 0.23738176f)))
      (n1502918575, n_36996899)    = n_1112360861.split(0.46712136f)
      n2137437950                 <- runSections(n1502918575, List((7, 0.8242769f)))
      n1281978524                 <- runSections(n_36996899, List((8, 0.67792714f)))
      n200080004                   = n1281978524 >|< n2137437950
      n_911563424                  = n_1885490992 >|< n200080004
      (n1009429810, n_984777102)   = n_911563424.split(0.15929568f)
      n1507742654                 <- runSections(n1009429810, List((2, 0.85035944f)))
      n_1346367828                <- runSections(n_984777102, List((3, 0.6530022f)))
      (n_1870652816, n_884652529)  = n1507742654.split(0.4179837f)
      n221340169                  <- runSections(n_1870652816, List((6, 0.707146f)))
      n1791511529                 <- runSections(n_884652529, List((4, 0.08678591f)))
      n1316284692                  = n1791511529 >|< n_1346367828
      (n_973792882, n123943753)    = n1316284692.split(0.5909483f)
      n_950423999                 <- runSections(n_973792882, List((7, 0.12110078f)))
      n830674740                  <- runSections(n123943753, List((4, 0.053336143f)))
      n1773789275                  = n830674740 >|< n_950423999
      (n_12835555, n_1845453989)   = n1773789275.split(0.6123032f)
      n1552341417                 <- runSections(n_12835555, List((10, 0.7348425f)))
      n470165866                  <- runSections(n_1845453989, List((10, 0.54476756f)))
      n_1739841122                 = n1552341417 >|< n470165866
      n_23410212                   = n_1739841122 >|< n221340169
      (n716266525, n285850228)     = n_23410212.split(0.38010907f)
      n2118929613                 <- runSections(n716266525, List((3, 0.085235894f)))
      n_328185564                 <- runSections(n285850228, List((10, 0.84769547f)))
      n_1099161676                 = n_328185564 >|< n2118929613
      (n_1858996816, n80197349)    = n_1099161676.split(0.24985152f)
      n110964627                  <- runSections(n_1858996816, List((14, 0.44141734f)))
      n_1585794645                <- runSections(n80197349, List((13, 0.16910058f)))
      n1008581210                  = n_1585794645 >|< n110964627
      (n_422046842, n_1538497912)  = n1008581210.split(0.11095542f)
      n_95554015                  <- runSections(n_422046842, List((15, 0.38963544f)))
      n1042570617                 <- runSections(n_1538497912, List((10, 0.23109722f)))
      n45761784                    = n1042570617 >|< n_95554015
      (n1464488337, n826889543)    = n45761784.split(0.23212159f)
      n_168127302                 <- runSections(n1464488337, List((10, 0.8466194f)))
      n_188358953                 <- runSections(n826889543, List((2, 0.8328306f)))
      (n_1391966993, n1671848291)  = n_168127302.split(0.25164324f)
      n1190750483                 <- runSections(n_1391966993, List((13, 0.32787257f)))
      n995028883                  <- runSections(n1671848291, List((8, 0.77417606f)))
      (n_1269818380, n335670858)   = n_188358953.split(0.08835584f)
      r                           <- runSections(n_1269818380, List((8, 0.07683784f)))
      _                           <- printDag(r, "regression/R01_LateMsgMismatch")
    } yield ()
  }
}
