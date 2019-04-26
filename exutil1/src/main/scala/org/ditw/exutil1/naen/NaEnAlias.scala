package org.ditw.exutil1.naen
import org.json4s.DefaultFormats

case class NaEnAlias(neid:Long, aliases:List[String]) {

}

object NaEnAlias extends Serializable {
  def load(json:String):Array[NaEnAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[NaEnAlias]]
  }
}
