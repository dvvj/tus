package org.ditw.pmxml.model

import java.util.Date

import com.lucidchart.open.xtract.{ XmlReader, __ }
import Constants.XmlTags._
import cats.syntax.all._

case class JournalInfo(
  ISSN: Option[String],
  title:String,
  pubDate:DateOrig
) {
  def getPubDate:DateCommon = {
    DateCommon.fromOrig(pubDate)
  }
}

object JournalInfo extends Serializable {
  implicit val reader:XmlReader[JournalInfo] = (
    (__ \ ISSN).read[String].optional,
    (__ \ Title).read[String],
    (__ \ JournalIssue \ PubDate).read[DateOrig]
  ).mapN(apply)
}