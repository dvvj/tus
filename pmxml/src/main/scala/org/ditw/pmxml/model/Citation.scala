package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import Constants.XmlTags._
import cats.syntax.all._
case class Citation(
  pmid:Int,
  journal:JournalInfo,
  _abstract:Abstract,
  authors:AuthorList,
  _artiDate:Option[DateOrig],
  meshHeadings:MeshHeadings
) {
  def getArticleDate:DateCommon = {
    if (_artiDate.nonEmpty && _artiDate.get.year.nonEmpty)
      DateCommon.fromOrig(_artiDate.get)
    else
      journal.getPubDate
  }
}

object Citation extends Serializable {
  implicit val reader:XmlReader[Citation] = (
      (__ \ PMID).read[Int],
      (__ \ Article \ Journal).read[JournalInfo],
      (__ \ Article \ _Abstract).read[Abstract],
      (__ \ Article \ _AuthorList).read[AuthorList],
      (__ \ Article \ ArticleDate).read[DateOrig].optional,
      (__ \ MeshHeadingList).read[MeshHeadings]
  ).mapN(apply)
}
