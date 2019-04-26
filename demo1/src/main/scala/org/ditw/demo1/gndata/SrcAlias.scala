package org.ditw.demo1.gndata
import org.json4s.DefaultFormats

case class SrcAlias(gnid:Long, aliases:List[String])

object SrcAlias extends Serializable {
  def load(json:String):Array[SrcAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[SrcAlias]]
  }
}