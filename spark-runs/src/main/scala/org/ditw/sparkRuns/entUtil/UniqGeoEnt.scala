package org.ditw.sparkRuns.entUtil
import org.json4s.DefaultFormats

case class UniqGeoEnt(
  id:Long,
  name:String,
  isni:Option[String],
  gnid:Long
) {

}

object UniqGeoEnt extends Serializable {
  def load(json:String):Array[UniqGeoEnt] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[UniqGeoEnt]]
  }
}