package org.ditw.sparkRuns.csvXtr.isniPrePrc
import org.json4s.DefaultFormats

case class IsniRelUnit(isni:String, n:String)

case class IsniRel(
  parent:IsniRelUnit,
  children:Array[String]
) {

}

object IsniRel extends Serializable {
  def load(json:String):Array[IsniRel] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniRel]]
  }

  def toJson(pocos:IsniRel):String = {
    import org.json4s.jackson.Serialization._
    writePretty(pocos)(DefaultFormats)
  }
}
