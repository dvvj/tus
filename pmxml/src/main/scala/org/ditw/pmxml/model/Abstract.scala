package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._
import Constants.XmlTags._
import cats.syntax.all._

case class Abstract(
                   texts:Seq[AbstractText]
                   ) {

}

object Abstract {
  implicit val reader:XmlReader[Abstract] = (
    (__ \ _AbstractText).read(seq[AbstractText].map(apply))
  )
}
