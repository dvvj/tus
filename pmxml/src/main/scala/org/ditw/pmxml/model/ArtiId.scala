package org.ditw.pmxml.model

import com.lucidchart.open.xtract.XmlReader.attribute
import com.lucidchart.open.xtract.{XmlReader, __}
import org.ditw.pmxml.model.Constants.XmlTags.IdType

import Constants.XmlTags._
import cats.syntax.all._

case class ArtiId(
                   idType:String,
                   idVal:String
                 )


object ArtiId extends Serializable {
  implicit val reader:XmlReader[ArtiId] = (
    (
      attribute[String](IdType),
      (__).read[String]
    )
    ).mapN(apply)
}
