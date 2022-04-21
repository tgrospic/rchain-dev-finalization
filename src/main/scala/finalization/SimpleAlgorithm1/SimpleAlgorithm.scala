package finalization.SimpleAlgorithm1

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import finalization.NetworkManager
import graphz.GraphGenerator.{dagAsCluster, ValidatorBlock}
import graphz.{GraphSerializer, ListSerializerRef}

import java.nio.file.Paths

object SimpleAlgorithm {
  type MsgView     = Message[String, Int]
  type SenderState = FinalizerState[String, Int]

  def genMessageId(sender: Int, height: Long) = s"$sender-$height"

  /**
    * Represents the state of the whole network with operations to split and join
    */
  final case class SimpleNetwork(senders: Set[SenderState]) {
    // Split network
    def split(perc: Float): (SimpleNetwork, SimpleNetwork) = {
      val total = senders.size
      val first = Math.round(total * perc)
      senders.splitAt(first).bimap(SimpleNetwork, SimpleNetwork)
    }

    def split(groups: Seq[Int]): Seq[SimpleNetwork] = {
      val (rest, nets) = groups.foldLeft((senders, Vector[SimpleNetwork]())) { case ((acc, r), n) =>
        val (sendersInGroup, rest) = acc.splitAt(n)

        assert(sendersInGroup.nonEmpty, "Network has no senders")

        val net = SimpleNetwork(sendersInGroup)
        (rest, r :+ net)
      }
      if (rest.nonEmpty) {
        nets :+ SimpleNetwork(rest)
      } else nets
    }

    // Merge networks
    def >|<(that: SimpleNetwork): SimpleNetwork = {
      val (s1, s2) = (senders, that.senders)
      // Exchange messages
      val msgs1    = s1.toSeq.flatMap(_.msgViewMap.values).toSet
      val msgs2    = s2.toSeq.flatMap(_.msgViewMap.values).toSet

      val newFor1 = msgs2 -- msgs1
      val newFor2 = msgs1 -- msgs2

      val newSenders1 =
        s1.map(s => newFor1.toSeq.sortBy(_.height).foldLeft(s)((acc, m) => acc.insertMsgView(m)._1))
      val newSenders2 =
        s2.map(s => newFor2.toSeq.sortBy(_.height).foldLeft(s)((acc, m) => acc.insertMsgView(m)._1))

      SimpleNetwork(newSenders1 ++ newSenders2)
    }

  }

  /**
    * Creates network with specified senders
    */
  def initNetwork(sendersCount: Int): SimpleNetwork = {
    // Arbitrary number of senders (bonded validators)
    val senders  = (0 until sendersCount).toSet
    // TODO: Assumes each sender with equal stake
    val bondsMap = senders.map((_, 1L)).toMap

    // Genesis message created by first sender
    val sender0       = senders.find(_ == 0).get
    // Genesis message id and height
    val genesisMsgId  = "g"
    val genesisHeight = 0L

    // Genesis message view
    val genesisView = Message(
      genesisMsgId,
      genesisHeight,
      sender0,
      senderSeq = -1,
      bondsMap = bondsMap,
      parents = Set(),
      // Fringe can be empty at start
      fringe = Set(),
      // fringe = Set(genesisMsgId),
      seen = Set(genesisMsgId)
    )

    // Latest messages from genesis validator
    val latestMsgs = Set(genesisView)

    // Seen message views
    val seenMsgViewMap = Map((genesisMsgId, genesisView))

    val senderStates =
      senders.map(s => FinalizerState(me = s, latestMsgs, seenMsgViewMap, genMessageId))

    SimpleNetwork(senderStates)
  }

  /**
    * Runs the network for number of rounds (heights) with skipped senders in each round
    *
    * @param network network to run
    * @param genHeight number of rounds to generate
    * @param skipPercentage percentage of senders not producing messages in a round
    * @return result network after run
    */
  def runNetwork[F[_]: Monad](network: SimpleNetwork, genHeight: Int, skipPercentage: Float): F[SimpleNetwork] =
    (genHeight, network).tailRecM { case (round, net) =>
      // Create messages randomly for each sender
      val newMsgSenders = net.senders.map { ss =>
        val rnd = Math.random()
        if (rnd > skipPercentage) ss.createMsgAndUpdateSender()
        else (ss, ss.msgViewMap.head._2)
      }

      // Collect all states for senders and messages
      val newSS   = newMsgSenders.map(_._1)
      val newMsgs = newMsgSenders.map(_._2)

      // Insert messages to all senders
      // - for message creators insert operation is idempotent
      val newSenderStates = newMsgs.foldLeft(newSS) { case (ss, m) =>
        ss.map(_.insertMsgView(m)._1)
      }

      // Update network with new sender states
      val newNet = net.copy(newSenderStates)

      val res =
        if (round > 1) (round - 1, newNet).asLeft // Loop
        else newNet.asRight                       // Final value

      res.pure[F]
    }

  /**
    * Network manager implementation for [[SimpleNetwork]]
    */
  final case class SimpleNetworkManager[F[_]: Sync]() extends NetworkManager[F] {
    type TyNet         = SimpleNetwork
    type TySenderState = SenderState

    override def getSenders(net: SimpleNetwork): Set[SenderState] = net.senders

    override def create(sendersCount: Int): SimpleNetwork = initNetwork(sendersCount)

    override def run(network: SimpleNetwork, genHeight: Int, skipPercentage: Float): F[SimpleNetwork] =
      runNetwork(network, genHeight, skipPercentage)

    override def split(net: SimpleNetwork, perc: Float): (SimpleNetwork, SimpleNetwork) = net.split(perc)

    override def split(net: SimpleNetwork, groups: Seq[Int]): Seq[SimpleNetwork] = net.split(groups)

    override def merge(net1: SimpleNetwork, net2: SimpleNetwork): SimpleNetwork = net1 >|< net2

    override def printDag(network: SimpleNetwork, name: String): F[Unit] = {
      val msgs = network.senders.head.msgViewMap.values.toList.map { m =>
        ValidatorBlock(m.id, m.sender.toString, m.height, m.parents.toList, m.fringe)
      }.toVector

      for {
        ref <- Ref[F].of(Vector[String]())
        _   <- {
          implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
          dagAsCluster[F](msgs)
        }
        res <- ref.get
        _    = {
          val graphString = res.mkString

          val filePrefix = s"vdags/$name"

          // Ensure directory exists
          val dir = Paths.get(filePrefix).getParent.toFile
          if (!dir.exists()) dir.mkdirs()

          // Save graph file
//          import java.nio.file._
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
  }
}
