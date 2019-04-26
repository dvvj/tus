package org.ditw.pmxml.model

import com.lucidchart.open.xtract.XmlReader.seq
import com.lucidchart.open.xtract.{XmlReader, __}
import Constants.XmlTags._
import cats.syntax.all._

case class MeshHeadings(
                       meshHeadings: Seq[MeshHeading]
                       ) {

}

object MeshHeadings extends Serializable {
  implicit val reader:XmlReader[MeshHeadings] = (
      (
        (__ \ _MeshHeading).read(seq[MeshHeading].map(apply))
      )
    )
}