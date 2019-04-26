package org.ditw.textSeg.common
import org.json4s.DefaultFormats

case class NamedSchoolCollege(
  name:String,
  affTo:String,
  aliases:Array[String]
) {

}

object NamedSchoolCollege extends Serializable {
  def fromJson(j:String):Array[NamedSchoolCollege] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[Array[NamedSchoolCollege]]
  }
}