package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._
import Constants.XmlTags._
import cats.syntax.all._

case class MeshHeading(
                      descriptor:MeshName,
                      qualifiers:Seq[MeshName]
                      ) {

}

object MeshHeading {
  implicit val reader:XmlReader[MeshHeading] = (
    (__ \ DescriptorName).read[MeshName],
    (__ \ QualifierName).read(seq[MeshName])
  ).mapN(apply)
}