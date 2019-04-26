package org.ditw.exutil1.poco
import org.json4s.DefaultFormats

case class PocoUS(name:String, gnid2Poco:Map[Long, Vector[String]]) {

}

object PocoUS extends Serializable {
  def toJson(poco:PocoUS):String = {
    import org.json4s.jackson.Serialization._
    write(poco)(DefaultFormats)
  }
  def toJsons(pocos:Array[PocoUS]):String = {
    import org.json4s.jackson.Serialization._
    writePretty(pocos)(DefaultFormats)
  }
  def fromJson(j:String):Array[PocoUS] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[Array[PocoUS]]
  }
}