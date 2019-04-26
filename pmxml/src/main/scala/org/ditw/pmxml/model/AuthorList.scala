package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._
import Constants.XmlTags._
import cats.syntax.all._

case class AuthorList(
  authors:Seq[Author]
) {

}

object AuthorList extends Serializable {
  implicit val reader:XmlReader[AuthorList] = (
    (
      (__ \ _Author).read(seq[Author].map(apply))
      )
    )
}