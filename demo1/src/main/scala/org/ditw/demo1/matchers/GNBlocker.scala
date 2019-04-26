package org.ditw.demo1.matchers
import org.json4s.DefaultFormats

case class GNBlocker(
  tag2Block:String,
  blockers:Array[String]
) {

}

object GNBlocker extends Serializable {
  def load(json:String):Array[GNBlocker] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[GNBlocker]]
  }
}