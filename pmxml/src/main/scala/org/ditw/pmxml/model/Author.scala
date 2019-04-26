package org.ditw.pmxml.model

import com.lucidchart.open.xtract.XmlReader._
import com.lucidchart.open.xtract.{XmlReader, __}
import org.ditw.pmxml.model.Constants.XmlTags._
import cats.syntax.all._

case class Author(
                 _isValid:String,
                 lastName:Option[String],
                 foreName:Option[String],
                 initials:Option[String],
                 collectiveName:Option[String],
                 affInfo:Seq[AffInfo],
                 idfr:Option[Identifier] = None
                 ) {
  val isValid:Boolean = _isValid == "Y"
  val isSingleAuthor:Boolean = collectiveName.isEmpty
  val isCollective:Boolean = collectiveName.nonEmpty
}

object Author extends Serializable {
  def fullName(_isValid:String,
    lastName:String,
    foreName:String,
    initials:String,
    affInfo:Seq[AffInfo],
    idfr:Option[Identifier] = None
  ) = Author(_isValid, Option(lastName), Option(foreName), Option(initials), None, affInfo, idfr)

  implicit val reader:XmlReader[Author] =
    (
      attribute[String](ValidYN),
      (__ \ LastName).read[String].optional,
      (__ \ ForeName).read[String].optional,
      (__ \ Initials).read[String].optional,
      (__ \ CollectiveName).read[String].optional,
      (__ \ AffiliationInfo).read(seq[AffInfo]),
      (__ \ _Identifier).read[Identifier].optional
    ).mapN(apply)
}