package org.ditw.sparkRuns.csvXtr
import org.json4s.DefaultFormats

case class IsniEnAlias(isni:String, aliases:List[String]) {

}

object IsniEnAlias extends Serializable {
  def load(json:String):Array[IsniEnAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniEnAlias]]
  }
}
