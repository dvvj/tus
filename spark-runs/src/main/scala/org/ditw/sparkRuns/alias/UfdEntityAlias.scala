package org.ditw.sparkRuns.alias

import org.json4s.DefaultFormats

case class UfdEntityAlias(neid:Long, aliases:Array[String]) {

}

object UfdEntityAlias extends Serializable {
  def load(json:String):Array[UfdEntityAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[UfdEntityAlias]]
  }
}