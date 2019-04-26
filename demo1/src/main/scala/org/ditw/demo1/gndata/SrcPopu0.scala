package org.ditw.demo1.gndata
import org.json4s.DefaultFormats

case class SrcPopu0(gnid:Long, name:String)

object SrcPopu0 extends Serializable {
  def load(j:String):Array[SrcPopu0] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[Array[SrcPopu0]]
  }
}
