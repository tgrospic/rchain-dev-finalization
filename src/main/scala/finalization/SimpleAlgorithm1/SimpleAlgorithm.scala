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

object SimpleAlgorithm {
  def showMsgs[M: Ordering, S: Ordering](ms: Seq[Message[M, S]]): String =
    ms.sortBy(x => (x.sender, x.height, x.id)).map(_.id).mkString(" ")

  def unfold[A, S](init: S)(f: S => Iterator[S]): LazyList[S] =
    LazyList.unfold(f(init)) { iter =>
      if (!iter.hasNext) none
      else {
        val x = iter.next()
        Some((x, iter ++ f(x)))
      }
    }

  case class Message[M, S](
      id: M,
      height: Long,
      sender: S,
      senderSeq: Long,
      bondsMap: Map[S, Long],
      parents: Set[M],
      fringe: Set[M],
      // Cache of seen message ids
      seen: Set[M]
  ) {
    override def hashCode(): Int = this.id.hashCode()
  }

  final case class Finalizer[M, S](msgViewMap: Map[M, Message[M, S]]) {
    // Iterate self parent messages
    def selfParents(mv: Message[M, S], finalized: Set[Message[M, S]]): Seq[Message[M, S]] =
      unfold(mv) { m =>
        m.parents.map(msgViewMap).filter(x => x.sender == mv.sender && !finalized(x)).toIterator
      }

    /**
      * Checks if minimum messages are enough for next fringe calculation
      */
    def checkMinMessages(minMsgs: List[Message[M, S]], bondsMap: Map[S, Long]): Boolean =
      // TODO: add support for epoch changes, simple comparison for senders count is not enough
      minMsgs.size == bondsMap.size

    /**
      * Finds top messages referenced from minimum messages (next message from each sender)
      */
    def calculateNextLayer(minMsgs: List[Message[M, S]]): Map[S, Message[M, S]] = {
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
      * Creates supported Map (senders see each other on next layer) for next fringe for each justification (sender)
      */
    def calculateNextFringeSupportMap(
        parents: Set[Message[M, S]],
        nextLayer: Map[S, Message[M, S]],
        finalized: Set[Message[M, S]]
    ): Map[S, Map[S, Set[S]]] =
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
      * Calculate next finalization fringe based on next fringe supported Map (senders see each other on next layer)
      */
    def calculateFringe(
        nextFringeSupportMap: Map[S, Map[S, Set[S]]],
        bondsMap: Map[S, Long]
    ): Boolean = {
      // Calculate stake supporting full partition
      val bondedSenders      = bondsMap.keySet
      val seeFullPartition   = nextFringeSupportMap
        .mapValues(x => x.nonEmpty && x.values.forall(_ == bondedSenders))
        .filter(_._2)
        .keys
      val fullPartitionStake = seeFullPartition.toSeq.map(bondsMap).sum
      // Total stake
      val totalStake         = bondsMap.values.toSeq.sum
      // Calculate if 2/3 of stake supporting next layer
      fullPartitionStake.toDouble / totalStake > 2d / 3
    }

    /**
      * Finalization logic
      *
      * @param justifications justification seen by the new message
      * @param bondsMap       bonds map used to validate messages built on parent fringe
      * @return fringe from joined justifications and a new detected fringe
      */
    def calculateFinalization(
        justifications: Set[Message[M, S]],
        bondsMap: Map[S, Long]
    ): (Set[Message[M, S]], Option[Set[Message[M, S]]]) = {
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

          // Create next fringe support Map map for each justification (sender)
          fringeSupportMap = calculateNextFringeSupportMap(justifications, nextLayer, parentFringe)

          // Calculate partition and resulting finalization fringe
          fringeFound = calculateFringe(fringeSupportMap, bondsMap)

          // Use next layer messages if fringe is detected
          fringe <- fringeFound.guard[Option].as(nextLayer.values.toSet)
        } yield fringe

      (parentFringe, newFringeOpt)
    }
  }

  // FinalizerState represents state of one validator in the network
  final case class FinalizerState[M: Ordering, S: Ordering](
      me: S,
      latestMsgs: Set[Message[M, S]],
      // Message view - updated when new message is added
      msgViewMap: Map[M, Message[M, S]] = Map(),
      genMsgId: (S, Long) => M
  ) {

    /**
      * Creates a new message, generates id (hash) and finalization fringe
      */
    def createMessageView(
        finalizer: Finalizer[M, S],
        height: Long,
        sender: S,
        senderSeq: Long,
        bondsMap: Map[S, Long],
        parents: Set[M]
    ): Message[M, S] = {
      // Message justifications
      val parentViews = parents.map(msgViewMap)

      // Calculate next fringe or continue with parent
      val (parentFringe, newFringeOpt) = finalizer.calculateFinalization(parentViews, bondsMap)
      val newFringe                    = newFringeOpt.getOrElse(parentFringe)
      val newFringeIds                 = newFringe.map(_.id)

      // Create a message view from a new received message
      val id = genMsgId(me, height) // s"$me-$height"

      // Seen messages are all seen from justifications combined
      val seenByParents = parentViews.flatMap(_.seen)
      val newSeen       = seenByParents + id

      // Create message view, an immutable object with all fields calculated
      val newMsgView =
        Message(id, height, sender, senderSeq, bondsMap, parents, fringe = newFringeIds, seen = newSeen)

      /* Debug log */
      if (me == sender) {
//        val fringeStr = newFringeOpt.map(ms => s"+ ${showMsgs(ms)}").getOrElse("")
//        println(s"${newMsgView.id} $fringeStr")
        debugLogMessage(finalizer, newMsgView, parentViews, parentFringe, newFringeOpt)
      }
      /* End Debug log */

      newMsgView
    }

    /**
      * Inserts a message to sender's state
      */
    def insertMsgView(msgView: Message[M, S]): (FinalizerState[M, S], Message[M, S]) =
      msgViewMap.get(msgView.id).map((this, _)).getOrElse {
        // Add message view to a view map
        val newMsgViewMap = msgViewMap + ((msgView.id, msgView))

        // Find latest message for sender
        val latest = latestMsgs.filter(_.sender == msgView.sender)

        // Update latest messages for sender
        val latestFromSender = msgView.senderSeq > latest.map(_.senderSeq).toList.maximumOption.getOrElse(-1L)
        val newLatestMsgs    =
          if (latestFromSender) latestMsgs -- latest + msgView
          else latestMsgs

        if (!latestFromSender)
          println(s"ERROR: add NOT latest message '${msgView.id}' for sender '$me''")

        // Create new sender state with added message
        val newState = copy(latestMsgs = newLatestMsgs, msgViewMap = newMsgViewMap)

        (newState, msgView)
      }

    /**
      * Creates a new message and adds it to sender state
      */
    def createMsgAndUpdateSender(): (FinalizerState[M, S], Message[M, S]) = {
      val maxHeight      = latestMsgs.map(_.height).max
      val newHeight      = maxHeight + 1
      val seqNum         = latestMsgs.find(_.sender == me).map(_.senderSeq).getOrElse(0L)
      val newSeqNum      = seqNum + 1
      val justifications = latestMsgs.map(_.id)
      // Bonds map taken from any latest message (assumes no epoch change happen)
      val bondsMap       = latestMsgs.head.bondsMap

      val finalizer = Finalizer(msgViewMap)

      // Create new message
      val newMsg = createMessageView(
        finalizer,
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
      * DEBUG: Prints debug output of a message (it has overhead of fringe support Map calculation)
      */
    def debugLogMessage(
        finalizer: Finalizer[M, S],
        msgView: Message[M, S],
        parentViews: Set[Message[M, S]],
        parentFringe: Set[Message[M, S]],
        newFringeOpt: Option[Set[Message[M, S]]]
    ): Unit = {
      def printNextFringeSupportMap(fringeSupportMap: Map[S, Map[S, Set[S]]]) =
        fringeSupportMap.toList
          .sortBy(_._1)
          .map { case (sp, seenByMap) =>
            val seenMapStr = seenByMap.toList
              .sortBy(_._1)
              .map { case (s, ss) =>
                val ssStr = ss.toList.sorted.mkString(", ")
                s"$s($ssStr)"
              }
              .mkString(" ")
            s"  $sp: $seenMapStr"
          }
          .mkString("\n")

      def debugInfo(parents: Set[Message[M, S]]) =
        for {
          // Find minimum message from each sender from justifications
          minMsgs <- parents.toList.traverse(finalizer.selfParents(_, parentFringe).lastOption)

          // Check if min messages satisfy requirements (senders in bonds map)
          _ <- finalizer.checkMinMessages(minMsgs, msgView.bondsMap).guard[Option]

          // Include ancestors of minimum messages as next layer
          nextLayer = finalizer.calculateNextLayer(minMsgs)

          // Create next fringe support Map map for each justification (sender)
          fringeSupportMap = finalizer.calculateNextFringeSupportMap(parents, nextLayer, parentFringe)

          // Debug print
          minMsgsStr       = showMsgs(minMsgs)
          nextLayerStr     = showMsgs(nextLayer.values.toSeq)
          fringeSupportStr = printNextFringeSupportMap(fringeSupportMap)
        } yield (nextLayerStr, fringeSupportStr, minMsgsStr)

      val (nextLayerStr, fringeSupportStr, minMsgsStr) = debugInfo(parentViews).getOrElse(("", "", ""))
      val (prefix, fringe)                             = newFringeOpt.map(("+", _)).getOrElse((":", parentFringe))
      val fringeStr                                    = showMsgs(fringe.toSeq)
      val parentFringeStr                              = showMsgs(parentFringe.toSeq)

      val printOutputs = Seq(nextLayerStr, fringeSupportStr, minMsgsStr, fringeStr, parentFringeStr)
      if (printOutputs.exists(_ != "")) {
        println(s"${me}: ${msgView.id}")
        println(s"SUPPORT:\n$fringeSupportStr")
        println(s"MIN    : $minMsgsStr")
        println(s"NEXT   : $nextLayerStr")
        println(s"PREV_F : $parentFringeStr")
        println(s"FRINGE $prefix $fringeStr")
        println(s"---------------------------------")
      }
    }
  }

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
      fringe = Set(genesisMsgId),
      // Fringe can be empty at start
//      fringe = Set(),
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
        ValidatorBlock(m.id, m.sender.toString, m.height, m.parents.toList)
      }.toVector

      for {
        ref <- Ref[F].of(Vector[String]())
        _   <- {
          implicit val ser: GraphSerializer[F] = new ListSerializerRef[F](ref)
          dagAsCluster[F](msgs, "")
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
