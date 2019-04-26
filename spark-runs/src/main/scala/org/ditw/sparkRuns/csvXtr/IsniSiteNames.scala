package org.ditw.sparkRuns.csvXtr
import org.json4s.DefaultFormats

case class IsniSiteNames(names:Array[String], siteNames:Array[String]) {

}

object IsniSiteNames extends Serializable {
  def load(json:String):Array[IsniSiteNames] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniSiteNames]]
  }
}
