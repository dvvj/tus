package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{ XmlReader, __ }
import com.lucidchart.open.xtract.XmlReader._
import Constants.XmlTags._
import cats.syntax.all._

case class AbstractText(
                       label:Option[String],
                       category:Option[String],
                       text:String
                       ) {

}

object AbstractText {
  implicit val reader:XmlReader[AbstractText] = (
    attribute[String](Label).optional,
    attribute[String](NlmCategory).optional,
    (__).read[String]
  ).mapN(apply)
}