package org.ditw.sparkRuns.alias

import org.json4s.DefaultFormats

case class UfdEntitySite(isni:String, n:String)
case class UfdEntitySiteAlias(names:Array[String], sites:Array[UfdEntitySite]) {

}

object UfdEntitySiteAlias extends Serializable {
  def load(json:String):Array[UfdEntitySiteAlias] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[UfdEntitySiteAlias]]
  }

  def toJson(siteAliases:Array[UfdEntitySiteAlias]):String = {
    implicit val fmt = DefaultFormats
    import org.json4s.jackson.Serialization._
    writePretty(siteAliases)
  }
}
