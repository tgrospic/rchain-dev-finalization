package graphz

import cats.effect.Sync
import cats.syntax.all._
import cats.{Applicative, Monad}

import scala.collection.compat.immutable.LazyList

object GraphGenerator {
  final case class ValidatorBlock(
      id: String,
      sender: String,
      height: Long,
      justifications: List[String],
      fringe: Set[String]
  )

  type ValidatorsBlocks = Map[Long, List[ValidatorBlock]]

  final case class DagInfo(
      validators: Map[String, ValidatorsBlocks],
      timeseries: Set[Long]
  )

  object DagInfo {
    def empty: DagInfo = DagInfo(validators = Map.empty, timeseries = Set.empty)
  }

  def dagAsCluster[F[_]: Sync: GraphSerializer](blocks: Vector[ValidatorBlock]): F[Graphz[F]] = {
    val acc            = blocks.foldLeft(DagInfo.empty)(accumulateDagInfo)
    val blockColorMap  = generateFringeColorMapping(blocks)
    val timeseries     = acc.timeseries.toList.sorted
    val lowestHeight   = timeseries.head
    val validators     = acc.validators
    val validatorsList = validators.toList.sortBy(_._1)
    for {
      g           <- initGraph[F]("dag")
      allAncestors = validatorsList
                       .flatMap { case (_, blocks) =>
                         blocks.get(lowestHeight).map(_.flatMap(b => b.justifications)).getOrElse(List.empty[String])
                       }
                       .distinct
                       .sorted

      // draw ancestors first
      _           <- allAncestors.traverse { ancestor =>
                       val (style, color) = styleForNode(ancestor, blockColorMap)
                       g.node(ancestor, shape = Box, style = style, color = color)
                     }

      // create invisible edges from ancestors to first node in each cluster for proper alignment
      _           <- validatorsList.traverse { case (valId, blocks) =>
                       allAncestors.traverse { ancestor =>
                         val nodes = nodesForHeight(lowestHeight, blocks, valId, blockColorMap).keys.toList
                         nodes.traverse(node => g.edge(ancestor, node, style = Some(Invis)))
                       }
                     }

      // draw clusters per validator
      _           <- validatorsList.traverse { case (id, blocks) =>
                       g.subgraph(
                         validatorCluster(id, blocks, timeseries, blockColorMap)
                       )
                     }

      // draw parent dependencies
      _           <- drawParentDependencies[F](g, validatorsList.map(_._2))

      // draw justification dotted lines
      showJustificationLines = true
      _                     <- if (!showJustificationLines)
                                 drawJustificationDottedLines[F](g, validators)
                               else
                                 ().pure[F]
      _                     <- g.close
    } yield g
  }

  private def accumulateDagInfo(
      acc: DagInfo,
      block: ValidatorBlock
  ): DagInfo = {
    val blockHeight     = block.height
    val validatorBlocks = Map(block.sender -> Map(blockHeight -> List(block)))
    acc
      .copy(
        timeseries = acc.timeseries + blockHeight,
        validators = acc.validators |+| validatorBlocks
      )
  }

  private def validatorCluster[G[_]: Monad: GraphSerializer](
      validatorId: String,
      blocks: ValidatorsBlocks,
      timeseries: List[Long],
      blockColorMap: Map[String, String]
  ): G[Graphz[G]] =
    for {
      g    <- Graphz.subgraph[G](s"cluster_$validatorId", DiGraph, label = Some(validatorId))
      nodes = timeseries.map(ts => nodesForHeight(ts, blocks, validatorId, blockColorMap))
      _    <- nodes.traverse(ns =>
                ns.toList.traverse { case (name, (style, color)) =>
                  // Node shape, style and color
                  g.node(name, shape = Box, style = style, color = color)
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
      graph = Map("fontsize" -> fontSize),
      node = Map("width" -> "0", "height" -> "0", "margin" -> "\".1,.05\"", "fontsize" -> fontSize),
      edge = Map(
        "arrowsize" -> ".5",
        // "arrowhead" -> "empty",
        "arrowhead" -> "open",
        "penwidth"  -> ".6"
        // "color"     -> "\"#404040\""
      )
    )
  }

  private def drawParentDependencies[G[_]: Applicative](
      g: Graphz[G],
      validators: List[ValidatorsBlocks]
  ): G[Unit] =
    validators
      .flatMap(_.values.toList.flatten)
      .traverse { case ValidatorBlock(id, _, _, justifications, _) =>
        justifications.traverse(p => g.edge(id, p, constraint = Some(false)))
      }
      .as(())

  private def drawJustificationDottedLines[G[_]: Applicative](
      g: Graphz[G],
      validators: Map[String, ValidatorsBlocks]
  ): G[Unit] =
    validators.values.toList
      .flatMap(_.values.toList.flatten)
      .traverse { case ValidatorBlock(id, _, _, justifications, _) =>
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

  /* Helper functions to generate block color mapping from fringes */

  private def generateFringeColorMapping(blocks: Vector[ValidatorBlock]): Map[String, String] = {
    // Different color for each fringe
    val colors        = LazyList(
      "#fdff7a", // yellow
      "#b6ff7a", // light green
      "#7affff", // light blue
      "#8f82ff", // purple
      "#fd82ff", // pink
      "#ff829b"  // red
      // dark colors
      // "#252dfa", // blue
      // "#19d900", // green
      // "#ababab"  // gray
    )
    val colorsInCycle = cycle(colors)

    // Collect all fringes, remove duplicates
    // TODO: sort fringes by height to prevent the same neighbour color
    val fringes = blocks.foldLeft(Set[Set[String]]()) { case (acc, b) => acc + b.fringe }

    // Zip fringes with colors
    fringes.zip(colorsInCycle).foldLeft(Map[String, String]()) { case (acc, (ids, color)) =>
      ids.foldLeft(acc) { case (acc1, id) => acc1 + ((id, color)) }
    }
  }

  private def cycle(xs: LazyList[String]): LazyList[String] = xs #::: cycle(xs)

  /* Helpers to generate node stype and color */

  // Creates map of node styles on block height
  private def nodesForHeight(
      height: Long,
      blocks: ValidatorsBlocks,
      validatorId: String,
      blockColorMap: Map[String, String]
  ): Map[String, (Option[GraphStyle], Option[String])] =
    transformOnHeight(height, blocks)(styleForNode(_, blockColorMap)).getOrElse(heightNoBlocks(height, validatorId))

  // Node style for a block
  private def styleForNode(blockId: String, blockColorMap: Map[String, String]): (Option[Filled.type], Option[String]) =
    blockColorMap
      .get(blockId)
      .map(color => (Filled.some, color.some))
      .getOrElse((none, none))

  // Node style on height without blocks
  private def heightNoBlocks(ts: Long, validatorId: String): Map[String, (Option[Invis.type], Option[String])] =
    Map(s"${ts.show}_$validatorId" -> (Some(Invis), none))

  // Transforms blocks on height
  private def transformOnHeight[A](height: Long, blocks: ValidatorsBlocks)(
      f: String => A
  ): Option[Map[String, A]] =
    blocks
      .get(height)
      .map { tsBlocks =>
        tsBlocks.map { case ValidatorBlock(id, _, _, _, _) => id -> f(id) }.toMap
      }
}
