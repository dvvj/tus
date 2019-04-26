package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._

import Constants.XmlTags._
import cats.syntax.all._

case class MeshName(
                   code:String,
                   _isMajor:String,
                   name:String
                   ) {
  val isMajor = _isMajor == "Y"
}

object MeshName {
  implicit val reader:XmlReader[MeshName] = (
    attribute[String](UI),
    attribute[String](MajorTopicYN),
    (__).read[String]
  ).mapN(apply)
}
