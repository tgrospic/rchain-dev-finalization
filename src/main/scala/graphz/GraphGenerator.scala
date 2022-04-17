package graphz

import cats.effect.Sync
import cats.syntax.all._
import cats.{Applicative, Monad}

object GraphGenerator {
  final case class ValidatorBlock(
      id: String,
      sender: String,
      height: Long,
      justifications: List[String]
  )

  type ValidatorsBlocks = Map[Long, List[ValidatorBlock]]

  final case class DagInfo(
      validators: Map[String, ValidatorsBlocks],
      timeseries: Set[Long]
  )

  object DagInfo {
    def empty: DagInfo = DagInfo(validators = Map.empty, timeseries = Set.empty)
  }

  def dagAsCluster[F[_]: Sync: GraphSerializer](
      blocks: Vector[ValidatorBlock], // Block hash
      lastFinalizedBlockHash: String
  ): F[Graphz[F]] =
    for {
      acc <- blocks.foldM(DagInfo.empty)(accumulateDagInfo[F](_, _))

      timeseries             = acc.timeseries.toList.sorted
      firstTs                = timeseries.head
      validators             = acc.validators
      validatorsList         = validators.toList.sortBy(_._1)
      g                     <- initGraph[F]("dag")
      allAncestors           = validatorsList
                                 .flatMap { case (_, blocks) =>
                                   blocks.get(firstTs).map(_.flatMap(b => b.justifications)).getOrElse(List.empty[String])
                                 }
                                 .distinct
                                 .sorted
      // draw ancesotrs first
      _                     <- allAncestors.traverse(ancestor =>
                                 g.node(
                                   ancestor,
                                   style = styleFor(ancestor, lastFinalizedBlockHash),
                                   shape = Box
                                 )
                               )
      // create invisible edges from ancestors to first node in each cluster for proper alligment
      _                     <- validatorsList.traverse { case (id, blocks) =>
                                 allAncestors.traverse { ancestor =>
                                   val nodes = nodesForTs(id, firstTs, blocks, lastFinalizedBlockHash).keys.toList
                                   nodes.traverse(node => g.edge(ancestor, node, style = Some(Invis)))
                                 }
                               }
      // draw clusters per validator
      _                     <- validatorsList.traverse { case (id, blocks) =>
                                 g.subgraph(
                                   validatorCluster(id, blocks, timeseries, lastFinalizedBlockHash)
                                 )
                               }
      // draw parent dependencies
      _                     <- drawParentDependencies[F](g, validatorsList.map(_._2))
      // draw justification dotted lines
      showJustificationLines = true
      _                     <- if (!showJustificationLines)
                                 drawJustificationDottedLines[F](g, validators)
                               else
                                 ().pure[F]
      _                     <- g.close
    } yield g

  private def accumulateDagInfo[F[_]: Sync](
      acc: DagInfo,
      block: ValidatorBlock
  ): F[DagInfo] = {
    val timeEntry       = block.height
    val validatorBlocks =
      Map(block.sender -> Map(timeEntry -> List(block)))
    acc
      .copy(
        timeseries = acc.timeseries + timeEntry,
        validators = acc.validators |+| validatorBlocks
      )
      .pure[F]
  }

  private def validatorCluster[G[_]: Monad: GraphSerializer](
      id: String,
      blocks: ValidatorsBlocks,
      timeseries: List[Long],
      lastFinalizedBlockHash: String
  ): G[Graphz[G]] =
    for {
      g    <- Graphz.subgraph[G](s"cluster_$id", DiGraph, label = Some(id))
      nodes = timeseries.map(ts => nodesForTs(id, ts, blocks, lastFinalizedBlockHash))
      _    <- nodes.traverse(ns =>
                ns.toList.traverse { case (name, style) =>
                  g.node(name, style = style, shape = Box)
                }
              )
      _    <- nodes.zip(nodes.drop(1)).traverse { case (n1s, n2s) =>
                n1s.keys.toList.traverse { n1 =>
                  n2s.keys.toList.traverse { n2 =>
                    g.edge(n1, n2, style = Some(Invis))
                  }

                }
              }
      _    <- g.close
    } yield g

  private def initGraph[G[_]: Monad: GraphSerializer](name: String): G[Graphz[G]] = {
    val fontSize = "10"
    Graphz[G](
      name,
      DiGraph,
      rankdir = Some(BT),
      splines = Some("false"),
      //      node = Map("width"     -> "0", "height" -> "0", "margin" -> ".03", "fontsize" -> "8"),
      graph = Map("fontsize" -> fontSize),
      node = Map("width" -> "0", "height" -> "0", "margin" -> "\".1,.05\"", "fontsize" -> fontSize),
      edge = Map(
        "arrowsize" -> ".5",
        //        "arrowhead" -> "empty",
        "arrowhead" -> "open",
        "penwidth"  -> ".6"
        //        "color"     -> "\"#404040\""
      )
    )
  }

  private def drawParentDependencies[G[_]: Applicative](
      g: Graphz[G],
      validators: List[ValidatorsBlocks]
  ): G[Unit] =
    validators
      .flatMap(_.values.toList.flatten)
      .traverse { case ValidatorBlock(id, _, _, justifications) =>
        justifications.traverse(p => g.edge(id, p, constraint = Some(false)))
      }
      .as(())

  private def drawJustificationDottedLines[G[_]: Applicative](
      g: Graphz[G],
      validators: Map[String, ValidatorsBlocks]
  ): G[Unit] =
    validators.values.toList
      .flatMap(_.values.toList.flatten)
      .traverse { case ValidatorBlock(id, _, _, justifications) =>
        justifications
          .traverse { j =>
            g.edge(
              id,
              j,
              style = Some(Dotted),
              constraint = Some(false),
              arrowHead = Some(NoneArrow)
            )
          }
      }
      .as(())

  private def nodesForTs(
      validatorId: String,
      ts: Long,
      blocks: ValidatorsBlocks,
      lastFinalizedBlockHash: String
  ): Map[String, Option[GraphStyle]] =
    blocks.get(ts) match {
      case Some(tsBlocks) =>
        tsBlocks.map { case ValidatorBlock(id, _, _, justifications) =>
          (id -> styleFor(id, lastFinalizedBlockHash))
        }.toMap
      case None           => Map(s"${ts.show}_$validatorId" -> Some(Invis))
    }

  private def styleFor(blockHash: String, lastFinalizedBlockHash: String): Option[GraphStyle] =
    if (blockHash == lastFinalizedBlockHash) Some(Filled) else None
}
