package org.ditw.demo1.gndata
import org.ditw.demo1.gndata.GNLevel.GNLevel

trait TGNColl extends Serializable {
  val level:GNLevel
  val self:Option[GNEnt]
  private[demo1] def subAdms:IndexedSeq[String]
  private[gndata] def updateSubAdms(newVal:IndexedSeq[String]):Unit
  val gents:Map[Long, GNEnt]

  val size:Int =
    if (self.nonEmpty) gents.size+1
    else gents.size

  def name2Id(admMap:Map[String, TGNColl]):Map[String, IndexedSeq[Long]]
  def id2Ent(admMap:Map[String, TGNColl]):Map[Long, GNEnt]
  protected def childAdms(m:Map[String, TGNColl]):Set[String] = {
    val c = subAdms.flatMap(sa => m(sa).childAdms(m))
    (c ++ subAdms).toSet
  }

  override def toString: String = {
    val selfName = if (self.nonEmpty) self.get.name else "_NA_"

    s"$level: $selfName(${subAdms.size},${gents.size})"
  }
  //protected[gndata] def childrenMap:Map[Long, GNEnt]
}
