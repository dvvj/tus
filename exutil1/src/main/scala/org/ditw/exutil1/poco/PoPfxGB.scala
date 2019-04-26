package org.ditw.exutil1.poco
import org.json4s.DefaultFormats

case class PoPfxGB(name:String, gnid2Pfxs:Map[Long, Vector[String]]) {

}

object PoPfxGB extends Serializable {
  def toJson(poPfx:PoPfxGB):String = {
    import org.json4s.jackson.Serialization._
    write(poPfx)(DefaultFormats)
  }
  def toJsons(poPfxs:Array[PoPfxGB]):String = {
    import org.json4s.jackson.Serialization._
    writePretty(poPfxs)(DefaultFormats)
  }

  def fromJson(j:String):Array[PoPfxGB] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[Array[PoPfxGB]]
  }
}