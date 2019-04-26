package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._
import org.ditw.pmxml.model.Constants.XmlTags.IdType
import Constants.XmlTags._
import cats.syntax.all._

case class Identifier(
  src:String,
  v:String
) {

}

object Identifier extends Serializable {
  implicit val reader:XmlReader[Identifier] = (
    (
      attribute[String](Source),
      (__).read[String]
    )
    ).mapN(apply)
}
