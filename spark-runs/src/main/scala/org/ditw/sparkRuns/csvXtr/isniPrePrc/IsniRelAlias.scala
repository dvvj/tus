package org.ditw.sparkRuns.csvXtr.isniPrePrc
import org.json4s.DefaultFormats

case class IsniRelAlias(isni:String, alias:String)

object IsniRelAlias extends Serializable {
  def load(json:String):Array[IsniRelAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniRelAlias]]
  }
}
