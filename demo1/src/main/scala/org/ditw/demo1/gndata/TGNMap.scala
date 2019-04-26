package org.ditw.demo1.gndata
import org.ditw.demo1.gndata.GNCntry.GNCntry

trait TGNMap extends TGNColl {
  def byId(gnid:Long):Option[GNEnt]
  def idsByName(name:String, adm:String):IndexedSeq[Long]
  def byName(name:String, adm:String):IndexedSeq[GNEnt]
  def byName(name:String):IndexedSeq[GNEnt]
  def admNameMap:Map[String, Map[String, IndexedSeq[Long]]]
  val admMap:Map[String, TGNColl]
  val countryCode:GNCntry
  val admIdMap:Map[String, Map[Long, GNEnt]]
  val idMap:Map[Long, GNEnt]

  def directSubAdms:Vector[String]

  override val size: Int = {
    val childrenCount = admMap.map(_._2.size).sum
    childrenCount+1 // country self entity always nonEmpty
  }
}
