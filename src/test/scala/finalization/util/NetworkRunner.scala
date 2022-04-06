package finalization.util

import cats.effect.Sync
import cats.syntax.all._
import finalization.NetworkManager
import finalization.SimpleAlgorithm1.SimpleAlgorithm._

import scala.util.Random

object NetworkRunner {

  /**
    * Instances for all supported network runners
    */
  implicit def runners[F[_]: Sync]: Seq[NetworkRunner[F]] = {
    val simpleAlg: NetworkManager[F] = SimpleNetworkManager[F]()

    Seq(NetworkRunner[F](simpleAlg))
  }
}

case class NetworkRunner[F[_]: Sync](netMngr: NetworkManager[F]) {
  // Network and Sender State types from Network Manager
  type Net         = netMngr.TyNet
  type SenderState = netMngr.TySenderState

  // Inline extensions for Network and Sender State
  // TODO: find a way to extract it to separate file
  class NetworkDataOps(net: Net) {
    def senders: Set[SenderState]         = netMngr.getSenders(net)
    def split(perc: Float): (Net, Net)    = netMngr.split(net, perc)
    def split(groups: Seq[Int]): Seq[Net] = netMngr.split(net, groups)
    def >|<(net2: Net): Net               = netMngr.merge(net, net2)
  }
  implicit def finalizationSyntaxNetworkData(net: Net): NetworkDataOps = new NetworkDataOps(net)

  /* Implementation */

  def printDag(network: Net, name: String): F[Unit] = netMngr.printDag(network, name)

  // Helper to initialize network
  def genNetwork(senders: Int): Net = netMngr.create(sendersCount = senders)

  /* Section runner (rounds with skipping some senders) */

  def runSections(start: Net, roundSkip: List[(Int, Float)]): F[Net] =
    roundSkip
      .foldM(start, 0) { case ((net, n), (height, skipP)) =>
        netMngr.run(net, height, skipP).map((_, n + 1))
      }
      .map(_._1)

  /* Infinite runner */

  def runInfinite(generateCode: Boolean): F[Unit] = {
    val nets           = List(10).map(genNetwork).map((_, "net"))
    val startIteration = 1
    (nets, startIteration).iterateForeverM { case (networks, iteration) =>
      if (!generateCode) {
        println(s"Iteration $iteration")
      }
      val newNetworks = splitMerge(networks, generateCode)
      newNetworks.map((_, iteration + 1)) // Infinite loop
    }
  }

  object Action extends Enumeration {
    val Split, Merge  = Value
    def random: Value = Random.nextInt(2) match {
      case 0 => Split
      case 1 => Merge
    }
  }

  private def splitMerge(
      nets: List[(Net, String)],
      generateCode: Boolean
  ): F[List[(Net, String)]] = {
    def removeByIndexFrom[T](v: List[T], i: Int): List[T] = v.patch(i, List.empty, 1)
    def uniqueNameFor[T](t: T): String                    =
      "n" + (t, Random.nextInt).hashCode.toString.replace("-", "_")

    // Runs simulation for network with random number of rounds and skip percent
    def runRounds(namedNet: (Net, String)): F[(Net, String)] = {
      val rounds = Random.nextInt(15) + 1 // from 1 to 15 inclusive
      val skip   = Random.nextFloat
      for {
        newNet <- runSections(namedNet._1, List((rounds, skip)))
      } yield {
        val newNetName = uniqueNameFor(newNet)
        if (generateCode) {
          println(
            s"""$newNetName <- runSections($namedNet, List(($rounds, ${skip}f)))""".stripMargin
          )
        }
        (newNet, newNetName)
      }
    }

    // Splits random network and runs random number of rounds for both parts
    def splitAndRun(nets: List[(Net, String)]): F[List[(Net, String)]] =
      for {
        index        <- Sync[F].delay(Random.nextInt(nets.size))
        splitPercent  = Random.nextFloat
        name          = nets(index)._2
        (left, right) = nets(index)._1.split(splitPercent)
        leftName      = uniqueNameFor(left)
        rightName     = uniqueNameFor(right)

        r <- if (left.senders.nonEmpty && right.senders.nonEmpty) {
               if (generateCode) {
                 println(s"($leftName, $rightName) = $name.split(${splitPercent}f)")
               }
               for {
                 leftNet  <- runRounds((left, leftName))
                 rightNet <- runRounds((right, rightName))
               } yield
               // Replace partition by index with leftNet and rightNet
               removeByIndexFrom(nets, index) :+ leftNet :+ rightNet
             } else {
               nets.pure
             }
      } yield r

    // Merges two random networks into one
    def merge(nets: List[(Net, String)]): F[List[(Net, String)]] = Sync[F].delay {
      if (nets.length >= 2) {
        // Take 2 random indices, remove corresponding items and add merging of them
        val indexes             = Random.shuffle(nets.indices.toList).take(2)
        val (leftNet, rightNet) = (nets(indexes.head)._1, nets(indexes(1))._1)
        val leftName            = nets(indexes.head)._2
        val rightName           = nets(indexes(1))._2
        val mergedNets          = leftNet >|< rightNet
        val mergedName          = uniqueNameFor(mergedNets)

        if (generateCode) {
          println(s"$mergedName = $leftName >|< $rightName")
        }

        nets.zipWithIndex
          .filter { case (_, index) => !indexes.contains(index) }
          .map { case (namedNet, _) => namedNet } :+ (mergedNets, uniqueNameFor(mergedNets))
      } else {
        nets
      }
    }

    for {
      act <- Action.random match {
               case Action.Split => splitAndRun(nets)
               case Action.Merge => merge(nets)
             }
    } yield act
  }

}
