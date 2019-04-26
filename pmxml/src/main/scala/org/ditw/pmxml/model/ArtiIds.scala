package org.ditw.pmxml.model

import com.lucidchart.open.xtract.XmlReader.seq
import com.lucidchart.open.xtract.{XmlReader, __}

import Constants.XmlTags._
import cats.syntax.all._
case class ArtiIds(
                    ids:Seq[ArtiId]
                  )

object ArtiIds extends Serializable {
  implicit val reader:XmlReader[ArtiIds] = (
    (
      (__ \ ArticleId).read(seq[ArtiId].map(apply))
      )
    )
}