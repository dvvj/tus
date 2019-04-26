package org.ditw.sparkRuns.entUtil
import org.json4s.DefaultFormats

case class NewEntInfo(gnid:Long, adm1:String, name:String, aliases:Array[String]) {

}

object NewEntInfo extends Serializable {
  def load(json:String):Array[NewEntInfo] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[NewEntInfo]]
  }
}