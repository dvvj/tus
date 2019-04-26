package org.ditw.pmxml.model
import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract.{ XmlReader, __ }
import Constants.XmlTags._
import cats.syntax.all._


case class DateOrig(
  dateType:Option[String],
                     year:Option[Int],
                     month:Option[String],
                     day:Option[Int],
                     medlineDate:Option[String]
                   )

object DateOrig {
  implicit val reader:XmlReader[DateOrig] = (
    attribute[String](DateType).optional,
    (__ \ Year).read[Int].optional,
    (__ \ Month).read[String].optional,
    (__ \ Day).read[Int].optional,
    (__ \ MedlineDate).read[String].optional
  ).mapN(apply)
}