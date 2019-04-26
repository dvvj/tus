package org.ditw.textSeg.output
import org.json4s.DefaultFormats

case class AffGN(
  gnid:Long,
  gnrep:String,
  pmAffFps:Seq[String]
) {
  val fpsCount:Int = pmAffFps.size
}
case class SegGN(
  name:String,
  affGns:Seq[AffGN]
) {
  val affCount:Int = affGns.size
  private val m = affGns.map(agn => agn.gnid -> agn).toMap
  def affGn(gnid:Long):AffGN = m(gnid)
}

object SegGN extends Serializable {
  def toJson(segGn:SegGN):String = {
    import org.json4s.jackson.Serialization._
    write(segGn)(DefaultFormats)
  }

  def fromJson(j:String):SegGN = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[SegGN]
  }
}