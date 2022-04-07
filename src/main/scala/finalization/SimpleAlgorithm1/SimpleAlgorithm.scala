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
  def showMsgs(ms: Set[Msg]) =
    ms.toSeq.sortBy(x => (x.height, x.id)).map(_.id).mkString(" ")

  def showMsgsSortSender(ms: Set[Msg]) =
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

  // Message exchanged between senders (validators)
  final case class Msg(
      id: String,
      height: Long,
      sender: Sender,
      senderSeq: Long,
      bondsMap: Map[Sender, Long],
      justifications: Map[Sender, String]
  ) {
    override def hashCode(): Int = this.id.hashCode()
  }

  // M |- root{ parents Final{ finalized } }
  case class MsgView(
      root: Msg,
      parents: Set[MsgView],
      fullFringe: Set[Msg],
      // Cache
      seen: Set[Msg]
  ) {
    override def hashCode(): Int = this.root.id.hashCode()

    override def toString: String = {
      val pMsgs         = parents.map(_.root)
      val pMsgsStr      = showMsgs(pMsgs)
      val fullFringeStr = showMsgs(fullFringe)

      s"${root.id} { $pMsgsStr, F{ $fullFringeStr } }"
    }
  }

  // SenderState represents state of one validator in the network
  final case class SenderState(
      me: Sender,
      latestMsgs: Map[Sender, Msg],
      dag: Map[String, Msg],
      heightMap: SortedMap[Long, Set[Msg]],
      // Message view - updated when new message is added
      msgViewMap: Map[Msg, MsgView] = Map()
  ) {
    override def hashCode(): Int = this.me.id.hashCode()

    def addMsg(msg: Msg): (SenderState, MsgView) =
      if (dag.contains(msg.id)) (this, msgViewMap(msg))
      else {
        /* Update sender state from a new received message */

        // Find latest message for sender
        val latest = latestMsgs.get(msg.sender)

        // Update latest message if newer
        val latestFromSender = msg.senderSeq > latest.map(_.senderSeq).getOrElse(-1L)
        val newLatestMsgs    =
          if (latestFromSender) latestMsgs + ((msg.sender, msg))
          else latestMsgs

        if (!latestFromSender)
          println(s"ERROR: add NOT latest message '${msg.id}' for sender '${me.id}''")

        // Update DAG
        val newDag = dag + ((msg.id, msg))

        // Update height map
        val heightSet    = heightMap.getOrElse(msg.height, Set())
        val newHeightMap = heightMap + ((msg.height, heightSet + msg))

        /* Create a message view from a new received message */

        // Update seen messages for message sender
        def loadMsgViews(m: Msg) =
          m.justifications.map { case (_, mid) => newDag(mid) }.map(msgViewMap(_)).toSet

        // Parent views for received message
        val msgParents = loadMsgViews(msg)

        // Latest full fringe seen from parents
        val parentFullFringe = msgParents.toList.maxBy(_.fullFringe.head.senderSeq).fullFringe

        // All seen messages from parents
        val seenByParents = msgParents.flatMap(_.seen)
        val newSeen       = seenByParents + msg

        // Finalized messages in from parents
        val finalized      = parentFullFringe
        val finalizedViews = finalized.map(msgViewMap)

        // Iterate self parent messages
        def selfParents(mv: MsgView) =
          unfold(mv) { m =>
            (m.parents.filter(_.root.sender == mv.root.sender) -- finalizedViews).toIterator
          }

        def checkMinMessages(minMsgs: List[MsgView]) =
          minMsgs.size == msg.bondsMap.size

        def calculateNextLayer(minMsgs: List[MsgView]) = {
          val minMessagesMap = minMsgs.map(x => (x.root.sender, x)).toMap
          minMsgs
            .flatMap(_.parents)
            .filter(x => minMessagesMap.keySet.contains(x.root.sender))
            .foldLeft(minMessagesMap) { case (acc, m) =>
              val currMin = acc(m.root.sender)
              val newMin  =
                if (m.root.senderSeq > currMin.root.senderSeq) m
                else currMin
              acc + ((m.root.sender, newMin))
            }
        }

        def calculateWitnessMap(parents: Set[MsgView], nextLayer: Map[Sender, MsgView]) =
          parents.map { mv =>
            val seenBy = nextLayer
              .map { case (s, minMsg) =>
                val seeMinMsg = mv.parents
                  .map { p =>
                    val parentSeen = (p +: selfParents(p)).exists(_.seen.contains(minMsg.root))
                    (p.root.sender, parentSeen)
                  }
                  .filter(_._2)
                  .toMap
                  .keySet
                (s, seeMinMsg)
              }
            (mv.root.sender, seenBy)
          }.toMap

        def calculateFringe(witnessMap: Map[Sender, Map[Sender, Set[Sender]]], nextLayer: Map[Sender, MsgView]) = {
          // Detect new full fringe
          // TODO: detect 2/3 of stake supporting partition
          val partitionMembers = witnessMap.keySet
          val hasNewFringe     = Seq(
            // All senders from bonds map form a partition
            () => partitionMembers.size == msg.bondsMap.size,
            // All senders witnessing partition
            () => witnessMap.size == msg.bondsMap.size,
            // All senders witnessing partition
            () => witnessMap.values.toSet.size == 1
          ).forall(_())

          // Use next layer messages if fringe is detected
          hasNewFringe
            .guard[Option]
            .as(
              nextLayer.mapValues(_.root).values.toSet
            )
        }

        def finalize(parents: Set[MsgView]) =
          for {
            // Find minimum message from each sender from justifications
            minMsgs <- parents.toList.traverse(selfParents(_).lastOption)

            // Check if min messages satisfy requirements (senders in bonds map)
            _ <- checkMinMessages(minMsgs).guard[Option]

            // Include ancestors of minimum messages as next layer
            nextLayer = calculateNextLayer(minMsgs)

            // Create witness map for each justification
            witnessMap = calculateWitnessMap(parents, nextLayer)

            // Calculate partition and resulting finalization fringe
            fringe <- calculateFringe(witnessMap, nextLayer)
          } yield fringe

        // Calculate next fringe or continue with parent
        val newFringeOpt  = finalize(msgParents)
        val newFullFringe = newFringeOpt.getOrElse(parentFullFringe)

        // Create message view object with all fields calculated
        val newMsgView = MsgView(root = msg, parents = msgParents, fullFringe = newFullFringe, seen = newSeen)

        // Add message view to a view map
        val newMsgViewMap = msgViewMap + ((msg, newMsgView))

        // Create new sender state with added message
        val newState = copy(
          latestMsgs = newLatestMsgs,
          dag = newDag,
          heightMap = newHeightMap,
          msgViewMap = newMsgViewMap
        )

        /* Debug log */

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
            minMsgs <- parents.toList.traverse(selfParents(_).lastOption)

            // Check if min messages satisfy requirements (senders in bonds map)
            _ <- checkMinMessages(minMsgs).guard[Option]

            // Include ancestors of minimum messages as next layer
            nextLayer = calculateNextLayer(minMsgs)

            // Create witness map for each justification
            witnessMap = calculateWitnessMap(parents, nextLayer)

            // Debug print
            nextLayerStr  = showMsgsSortSender(nextLayer.mapValues(_.root).values.toSet)
            witnessMapStr = printWitnessMap(witnessMap)
          } yield (nextLayerStr, witnessMapStr)

        val (nextLayerStr, witnessesStr) = debugInfo(msgParents).getOrElse(("-", "-"))
        val (prefix, fringe)             = newFringeOpt.map(("+", _)).getOrElse((":", parentFullFringe))
        val fringeStr                    = showMsgsSortSender(fringe)

        if (me == msg.sender) {
          println(s"${me.id}: ${msg.id}")
          println(s"WITNESS:\n$witnessesStr")
          println(s"NEXT   : $nextLayerStr")
          println(s"FRINGE $prefix $fringeStr")
          println(s"---------------------------------")
        }

        (newState, newMsgView)
      }

    def createMsg(): (SenderState, MsgView) = {
      val maxHeight      = latestMsgs.map(_._2.height).max
      val newHeight      = maxHeight + 1
      val seqNum         = latestMsgs.get(me).map(_.senderSeq).getOrElse(0L)
      val newSeqNum      = seqNum + 1
      val justifications = latestMsgs.map { case (s, m) => (s, m.id) }
      // Bonds map taken from any latest message (assumes no epoch change happen)
      val bondsMap       = latestMsgs.head._2.bondsMap

      // Create new message
      val newMsg = Msg(
        id = s"${me.id}-$newHeight",
        height = newHeight,
        sender = me,
        senderSeq = newSeqNum,
        bondsMap,
        justifications
      )

      // Add message to self state
      addMsg(newMsg)
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
      val msgs1    = s1.flatMap(_.dag.values)
      val msgs2    = s2.flatMap(_.dag.values)

      val newSenders1 =
        s1.map(s => msgs2.toList.sortBy(x => x.height).foldLeft(s)((acc, m) => acc.addMsg(m)._1))
      val newSenders2 =
        s2.map(s => msgs1.toList.sortBy(x => x.height).foldLeft(s)((acc, m) => acc.addMsg(m)._1))

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
    val sender0    = senders.find(_.id == 0).get
    val genesisMsg =
      Msg(s"g", height = 0, sender = sender0, senderSeq = -1, justifications = Map(), bondsMap = bondsMap)

    // Latest messages from genesis validator
    val latestMsgs = Map((genesisMsg.sender, genesisMsg))

    // Initial messages in the DAG
    val dag = Map((genesisMsg.id, genesisMsg))

    // Initial height map including genesis
    val heightMap = SortedMap(genesisMsg.height -> Set(genesisMsg))

    // Seen state with genesis message
    val seenMap = Map(
      genesisMsg -> MsgView(
        root = genesisMsg,
        parents = Set(),
        fullFringe = Set(genesisMsg),
        seen = Set(genesisMsg)
      )
    )

    val senderStates =
      senders.map(s => SenderState(me = s, latestMsgs, dag, heightMap, seenMap))

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
      val newMsgSenders = net.senders.map { ss =>
        val rnd       = Math.random()
        val (newS, m) =
          if (rnd > skipPercentage) ss.createMsg()
          else (ss, ss.msgViewMap.head._2)
        (newS, m.root)
      }

      val newSS   = newMsgSenders.map(_._1)
      val newMsgs = newMsgSenders.map(_._2)

      val newSenderStates = newMsgs.foldLeft(newSS) { case (ss, m) =>
        ss.map(_.addMsg(m)._1)
      }

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
            ValidatorBlock(m.id, m.sender.id.toString, m.height, m.justifications.values.toList)
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
