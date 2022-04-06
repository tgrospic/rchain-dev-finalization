package finalization

trait NetworkManager[F[_]] {
  // Network type
  type TyNet
  // Sender state type
  type TySenderState

  // Creation
  def create(sendersCount: Int): TyNet

  // Running
  def run(network: TyNet, genHeight: Int, skipPercentage: Float): F[TyNet]

  // Network manipulation (branch, join)
  def split(net: TyNet, perc: Float): (TyNet, TyNet)
  def split(net: TyNet, groups: Seq[Int]): Seq[TyNet]
  def merge(net1: TyNet, net2: TyNet): TyNet

  // Printer
  def printDag(net: TyNet, name: String): F[Unit]

  // Field access
  def getSenders(net: TyNet): Set[TySenderState]
}
