package finalization.util

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import finalization.Finalization._
import graphz.GraphGenerator._
import graphz._

import scala.util.Random

object NetworkRunner {}

class NetworkRunner[F[_]: Sync](enableOutput: Boolean) {

  def printDag(network: Network, name: String) = {
    // DAG toposort
    val msgs = network.senders.head.heightMap
    val topo = msgs.map { case (_, v) => v.toVector }.toVector

    for {
      ref <- Ref[F].of(Vector[String]())
      _   <- {
        implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
        dagAsCluster[F](topo, "")
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

  def runSections(
      start: Network,
      roundSkip: List[(Int, Float)],
      prefix: String
  ) = {
    val senderCount = start.senders.size
    roundSkip.foldM(start, 0) { case ((net, n), (height, skipP)) =>
      runNetwork(net, height, skipP).flatMap { res =>
        //            val name = s"$prefix-$n-$senderCount-$skipP"
        val name = s"$prefix-$n-$senderCount"

        printDag(res, name).whenA(enableOutput).as((res, n + 1))
      }
    }
  }

  // Helper to initialize network
  def genNet(senders: Int) =
    initNetwork(sendersCount = senders) -> s"net$senders"

  /* Regression runner (run previously failed tests) */

  // Regression test was derived from infinite test via scala code printing
  def runRegression(f: (NetworkRunner[F], Network, String) => F[(Network, Int)]) = {
    val (net, name) = genNet(10)
    f(this, net, name)
  }

  /* Infinite runner */

  def runInfinite(generateCode: Boolean): F[Unit] = {
    val nets           = List(10).map(genNet)
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
      nets: List[(Network, String)],
      generateCode: Boolean
  ): F[List[(Network, String)]] = {
    def removeByIndexFrom[T](v: List[T], i: Int): List[T] = v.patch(i, List.empty, 1)
    def uniqueNameFor[T](t: T): String                    =
      "n" + (t, Random.nextInt).hashCode.toString.replace("-", "_")

    // Runs simulation for network with random number of rounds and skip percent
    def runRounds(namedNet: (Network, String)): F[(Network, String)] = {
      val rounds = Random.nextInt(15) + 1 // from 1 to 15 inclusive
      val skip   = Random.nextFloat
      for {
        r <- runSections(namedNet._1, List((rounds, skip)), namedNet._2)
      } yield {
        val newNet     = r._1
        val newNetName = uniqueNameFor(newNet)
        if (generateCode) {
          println(
            s"""${newNetName}_ <- runSections(${namedNet._2}, List(($rounds, ${skip}f)), "${namedNet._2}")""".stripMargin
          )
          println(s"($newNetName, _) = ${newNetName}_")
        }
        (newNet, newNetName)
      }
    }

    // Splits random network and runs random number of rounds for both parts
    def splitAndRun(nets: List[(Network, String)]): F[List[(Network, String)]] =
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
    def merge(nets: List[(Network, String)]): F[List[(Network, String)]] = Sync[F].delay {
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
