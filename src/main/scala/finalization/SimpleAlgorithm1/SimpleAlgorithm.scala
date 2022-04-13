package finalization.SimpleAlgorithm1

import cats.Monad
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import finalization.NetworkManager
import graphz.GraphGenerator.{dagAsCluster, ValidatorBlock}
import graphz.{GraphSerializer, ListSerializerRef}

import java.nio.file.Paths
import scala.collection.compat.immutable.LazyList
import scala.collection.immutable.SortedMap

object SimpleAlgorithm {
  def showMsgs(ms: Set[MsgView]) =
    ms.toSeq.sortBy(x => (x.sender.id, x.height, x.id)).map(_.id).mkString(" ")

  def unfold[A, S](init: S)(f: S => Iterator[S]) =
    LazyList.unfold(f(init)) { iter =>
      if (!iter.hasNext) none
      else {
        val x = iter.next()
        Some((x, iter ++ f(x)))
      }
    }

  // Sender represents validator node
  final case class Sender(id: Int) {
    override def hashCode(): Int = this.id.hashCode()
  }

  // M |- root{ parents Final{ finalized } }
  case class MsgView(
      id: String,
      height: Long,
      sender: Sender,
      senderSeq: Long,
      bondsMap: Map[Sender, Long],
      parents: Set[String],
      fringe: Set[String],
      // Cache of seen message ids
      seen: Set[String]
  ) {
    override def hashCode(): Int = this.id.hashCode()
  }

  // SenderState represents state of one validator in the network
  final case class SenderState(
      me: Sender,
      latestMsgs: Map[Sender, MsgView],
      heightMap: SortedMap[Long, Set[MsgView]],
      // Message view - updated when new message is added
      msgViewMap: Map[String, MsgView] = Map()
  ) {
    // Iterate self parent messages
    def selfParents(mv: MsgView, finalized: Set[MsgView]): Seq[MsgView] =
      unfold(mv) { m =>
        m.parents.map(msgViewMap).filter(x => x.sender == mv.sender && !finalized(x)).toIterator
      }

    /**
      * Checks is minimum messages are enough for next fringe calculation
      */
    def checkMinMessages(minMsgs: List[MsgView], bondsMap: Map[Sender, Long]): Boolean =
      // TODO: add support for epoch changes, simple comparison for senders count is not enough
      minMsgs.size == bondsMap.size

    /**
      * Finds top messages referenced from minimum messages (next message from each sender)
      */
    def calculateNextLayer(minMsgs: List[MsgView]): Map[Sender, MsgView] = {
      val minMessagesMap = minMsgs.map(x => (x.sender, x)).toMap
      minMsgs
        .flatMap(_.parents.map(msgViewMap))
        .filter(x => minMessagesMap.keySet.contains(x.sender))
        .foldLeft(minMessagesMap) { case (acc, m) =>
          val currMin = acc(m.sender)
          val newMin  =
            if (m.senderSeq > currMin.senderSeq) m
            else currMin
          acc + ((m.sender, newMin))
        }
    }

    /**
      * Creates witness map for each justification
      */
    def calculateWitnessMap(
        parents: Set[MsgView],
        nextLayer: Map[Sender, MsgView],
        finalized: Set[MsgView]
    ): Map[Sender, Map[Sender, Set[Sender]]] =
      parents.map { mv =>
        val seenBy = nextLayer
          .map { case (s, minMsg) =>
            // Search parents of parent if not part of next layer
            val parentsOfParent = mv.parents -- nextLayer.values.map(_.id)
            val seeMinMsg       = parentsOfParent
              .map(msgViewMap)
              .map { p =>
                // Find if next layer message is seen from any parent message
                val selfMsgs     = p +: selfParents(p, finalized)
                val seenByParent = selfMsgs.exists(_.seen.contains(minMsg.id))
                (p.sender, seenByParent)
              }
              .filter(_._2)
              .toMap
              .keySet
            (s, seeMinMsg)
          }
          .filter(_._2.nonEmpty)
        (mv.sender, seenBy)
      }.toMap

    /**
      * Calculate next finalization fringe based on witness map
      */
    def calculateFringe(witnessMap: Map[Sender, Map[Sender, Set[Sender]]], bondsMap: Map[Sender, Long]): Boolean = {
      // Check requirements to validate the next fringe witnessed by 2/3 of the stake
      val bondedSenders            = bondsMap.keySet
      val witnessesOfFullPartition = witnessMap.mapValues(_.values.forall(_ == bondedSenders))
      val witnessStake             = witnessesOfFullPartition.filter(_._2).keysIterator.map(bondsMap).sum
      val totalStake               = bondsMap.values.sum
      // Calculate if 2/3 of witnessed stake supporting next layer
      witnessStake.toDouble / totalStake > 2d / 3
    }

    /**
      * Finalization logic
      *
      * @param justifications justification seen by the new message
      * @param bondsMap bonds map seen by the new message
      * @return fringe from joined justifications and a new detected fringe
      */
    def calculateFinalization(
        justifications: Set[MsgView],
        bondsMap: Map[Sender, Long]
    ): (Set[MsgView], Option[Set[MsgView]]) = {
      // Latest fringe seen from justifications
      // - can be empty which means first layer is first message from each sender
      val parentFringe =
        justifications.toList
          .maxBy(_.fringe.map(msgViewMap).headOption.map(_.senderSeq).getOrElse(-1L))
          .fringe
          .map(msgViewMap)

      val newFringeOpt =
        for {
          // Find minimum message from each sender from justifications
          minMsgs <- justifications.toList.traverse(selfParents(_, parentFringe).lastOption)

          // Check if min messages satisfy requirements (senders in bonds map)
          _ <- checkMinMessages(minMsgs, bondsMap).guard[Option]

          // Include ancestors of minimum messages as next layer
          nextLayer = calculateNextLayer(minMsgs)

          // Create witness map for each justification
          witnessMap = calculateWitnessMap(justifications, nextLayer, parentFringe)

          // Calculate partition and resulting finalization fringe
          fringeFound = calculateFringe(witnessMap, bondsMap)

          // Use next layer messages if fringe is detected
          fringe <- fringeFound.guard[Option].as(nextLayer.values.toSet)
        } yield fringe

      (parentFringe, newFringeOpt)
    }

    /**
      * Creates a new message, generates id (hash) and finalization fringe
      */
    def createMessageView(
        height: Long,
        sender: Sender,
        senderSeq: Long,
        bondsMap: Map[Sender, Long],
        parents: Set[String]
    ): MsgView = {
      // Message justifications
      val parentViews = parents.map(msgViewMap)

      // Calculate next fringe or continue with parent
      val (parentFringe, newFringeOpt) = calculateFinalization(parentViews, bondsMap)
      val newFringe                    = newFringeOpt.getOrElse(parentFringe)
      val newFringeIds                 = newFringe.map(_.id)

      // Create a message view from a new received message
      val id = s"${me.id}-$height"

      // Seen messages are all seen from justifications combined
      val seenByParents = parentViews.flatMap(_.seen)
      val newSeen       = seenByParents + id

      // Create message view object with all fields calculated
      val newMsgView =
        MsgView(id, height, sender, senderSeq, bondsMap, parents, fringe = newFringeIds, seen = newSeen)

      /* Debug log */
      if (me == sender) {
//        val fringeStr = newFringeOpt.map(ms => s"+ ${showMsgs(ms)}").getOrElse("")
//        println(s"${newMsgView.id} $fringeStr")
        debugLogMessage(newMsgView, parentViews, parentFringe, newFringeOpt)
      }
      /* End Debug log */

      newMsgView
    }

    /**
      * Creates a new message and adds it to sender state
      */
    def createMsgAndUpdateSender(): (SenderState, MsgView) = {
      val maxHeight      = latestMsgs.map(_._2.height).max
      val newHeight      = maxHeight + 1
      val seqNum         = latestMsgs.get(me).map(_.senderSeq).getOrElse(0L)
      val newSeqNum      = seqNum + 1
      val justifications = latestMsgs.map { case (_, m) => m.id }.toSet
      // Bonds map taken from any latest message (assumes no epoch change happen)
      val bondsMap       = latestMsgs.head._2.bondsMap

      // Create new message
      val newMsg = createMessageView(
        height = newHeight,
        sender = me,
        senderSeq = newSeqNum,
        bondsMap,
        justifications
      )

      // Insert message view to self state
      insertMsgView(newMsg)
    }

    /**
      * Inserts a message to sender's state
      */
    def insertMsgView(msgView: MsgView): (SenderState, MsgView) =
      msgViewMap.get(msgView.id).map((this, _)).getOrElse {
        // Add message view to a view map
        val newMsgViewMap = msgViewMap + ((msgView.id, msgView))

        // Find latest message for sender
        val latest = latestMsgs.get(msgView.sender)

        // Update latest messages
        val latestFromSender = msgView.senderSeq > latest.map(_.senderSeq).getOrElse(-1L)
        val newLatestMsgs    =
          if (latestFromSender) latestMsgs + ((msgView.sender, msgView))
          else latestMsgs

        if (!latestFromSender)
          println(s"ERROR: add NOT latest message '${msgView.id}' for sender '${me.id}''")

        // Update height map
        val heightSet    = heightMap.getOrElse(msgView.height, Set())
        val newHeightMap = heightMap + ((msgView.height, heightSet + msgView))

        // Create new sender state with added message
        val newState = copy(latestMsgs = newLatestMsgs, heightMap = newHeightMap, msgViewMap = newMsgViewMap)

        (newState, msgView)
      }

    /**
      * DEBUG: Prints debug output of a message (it has overhead of witness calculation)
      */
    def debugLogMessage(
        msgView: MsgView,
        parentViews: Set[MsgView],
        parentFringe: Set[MsgView],
        newFringeOpt: Option[Set[MsgView]]
    ): Unit = {
      def printWitnessMap(witnessMap: Map[Sender, Map[Sender, Set[Sender]]]) =
        witnessMap.toList
          .sortBy(_._1.id)
          .map { case (sp, seenByMap) =>
            val seenMapStr = seenByMap.toList
              .sortBy(_._1.id)
              .map { case (s, ss) =>
                val ssStr = ss.toList.sortBy(_.id).map(_.id).mkString(", ")
                s"${s.id}($ssStr)"
              }
              .mkString(" ")
            s"  ${sp.id}: $seenMapStr"
          }
          .mkString("\n")

      def debugInfo(parents: Set[MsgView]) =
        for {
          // Find minimum message from each sender from justifications
          minMsgs <- parents.toList.traverse(selfParents(_, parentFringe).lastOption)

          // Check if min messages satisfy requirements (senders in bonds map)
          _ <- checkMinMessages(minMsgs, msgView.bondsMap).guard[Option]

          // Include ancestors of minimum messages as next layer
          nextLayer = calculateNextLayer(minMsgs)

          // Create witness map for each justification
          witnessMap = calculateWitnessMap(parents, nextLayer, parentFringe)

          // Debug print
          minMsgsStr    = showMsgs(minMsgs.toSet)
          nextLayerStr  = showMsgs(nextLayer.values.toSet)
          witnessMapStr = printWitnessMap(witnessMap)
        } yield (nextLayerStr, witnessMapStr, minMsgsStr)

      val (nextLayerStr, witnessesStr, minMsgsStr) = debugInfo(parentViews).getOrElse(("", "", ""))
      val (prefix, fringe)                         = newFringeOpt.map(("+", _)).getOrElse((":", parentFringe))
      val fringeStr                                = showMsgs(fringe)
      val parentFringeStr                          = showMsgs(parentFringe)

      val printOutputs = Seq(nextLayerStr, witnessesStr, minMsgsStr, fringeStr, parentFringeStr)
      if (printOutputs.exists(_ != "")) {
        println(s"${me.id}: ${msgView.id}")
        println(s"WITNESS:\n$witnessesStr")
        println(s"MIN    : $minMsgsStr")
        println(s"NEXT   : $nextLayerStr")
        println(s"PREV_F : $parentFringeStr")
        println(s"FRINGE $prefix $fringeStr")
        println(s"---------------------------------")
      }
    }
  }

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
    val senders  = (0 until sendersCount).map(Sender).toSet
    // TODO: Assumes each sender with equal stake
    val bondsMap = senders.map((_, 1L)).toMap

    // Genesis message created by first sender
    val sender0       = senders.find(_.id == 0).get
    // Genesis message id and height
    val genesisMsgId  = "g"
    val genesisHeight = 0L

    // Genesis message view
    val genesisView = MsgView(
      genesisMsgId,
      genesisHeight,
      sender0,
      senderSeq = -1,
      bondsMap = bondsMap,
      parents = Set(),
      fringe = Set(genesisMsgId),
      // Fringe can be empty at start
//      fringe = Set(),
      seen = Set(genesisMsgId)
    )

    // Latest messages from genesis validator
    val latestMsgs = Map((sender0, genesisView))

    // Initial height map including genesis
    val heightMap = SortedMap(genesisHeight -> Set(genesisView))

    // Seen message views
    val seenMsgViewMap = Map((genesisMsgId, genesisView))

    val senderStates =
      senders.map(s => SenderState(me = s, latestMsgs, heightMap, seenMsgViewMap))

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
      // DAG toposort
      val msgs = network.senders.head.heightMap
      val topo = msgs.map { case (_, v) =>
        v.toVector
          .map { m =>
            ValidatorBlock(m.id, m.sender.id.toString, m.height, m.parents.toList)
          }
      }.toVector

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

          // Ensure directory exists
          val dir = Paths.get(filePrefix).getParent.toFile
          if (!dir.exists()) dir.mkdirs()

          // Save graph file
          //  Files.writeString(Path.of(s"$filePrefix.dot"), graphString)

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
