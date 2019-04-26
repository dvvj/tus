package org.ditw.sparkRuns.csvXtr
import org.json4s.DefaultFormats

case class IsniBlocked(isni:String, n:String) {

}

object IsniBlocked extends Serializable {
  def load(json:String):Array[IsniBlocked] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniBlocked]]
  }
}