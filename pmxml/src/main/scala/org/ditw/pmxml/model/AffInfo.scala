package org.ditw.pmxml.model

import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract.{XmlReader, __}
import org.ditw.pmxml.model.Constants.XmlTags._
import cats.syntax.all._

case class AffInfo(
  aff:String,
  idfrs:Seq[Identifier] = AffInfo.EmptyIds
) {
  //def this(aff:String) = this(aff, Seq())
}

object AffInfo extends Serializable {
  private val EmptyIds = Seq[Identifier]()
  implicit val reader:XmlReader[AffInfo] =
    (
      (__ \ Affiliation).read[String],
      (__ \ _Identifier).read(seq[Identifier])
    ).mapN(apply)
}
