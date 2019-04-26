package org.ditw.demo1.gndata
import org.ditw.demo1.gndata.GNLevel.GNLevel

case class GNEnt(
  gnid:Long,
  name:String,
  private[gndata] var _alias:Set[String],
  latitude:Double,
  longitude:Double,
  featureClz:String,
  featureCode:String,
  countryCode:String,
  admCodes:IndexedSeq[String],
  population:Long
) {
  val admc:String = admCodes.mkString("_")
  override def toString: String = {
    val coord = f"{$latitude%.3f,$longitude%.3f}"
    s"$name([$featureCode]$gnid: $admc,$coord)"
  }
//  def toStringJson: String = {
//    val coord = f"{$latitude%.3f-$longitude%.3f}"
//    s"$name| $featureCode| $gnid| $admc| $coord)"
//
//  }
  private var _queryNames:Set[String] = (_alias + name).filter(!_.isEmpty).map(_.toLowerCase())
  def queryNames:Set[String] = _queryNames
  def addAliases(aliases:Iterable[String]):Unit = {
    _alias ++= aliases
//    if (level == GNLevel.ADM1)
//      println("ok")
    _queryNames ++= aliases.filter(!_.isEmpty).map(_.toLowerCase())
  }
  val isAdm:Boolean = featureClz == "A"
  val admLevel:Int = admCodes.size
  val level:GNLevel = admCodes.size match {
    case 0 => GNLevel.ADM0
    case 1 => GNLevel.ADM1
    case 2 => GNLevel.ADM2
    case 3 => GNLevel.ADM3
    case 4 => GNLevel.ADM4
  }

  override def hashCode(): Int = gnid.toInt
  override def equals(obj: Any): Boolean = {
    obj match {
      case e:GNEnt => e.gnid == gnid
      case _ => false
    }
  }
}
