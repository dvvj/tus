package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{XmlReader, __}
import com.lucidchart.open.xtract.XmlReader._
import Constants.XmlTags._
import cats.syntax.all._
import org.ditw.pmxml.model.Arti._fmts
import org.json4s.DefaultFormats


case class ArtiSet(artis:Seq[Arti]) {

}

object ArtiSet extends Serializable {
  val EmptyArtiSet = ArtiSet(Seq())

  implicit val reader:XmlReader[ArtiSet] = (
    (__ \ PubmedArticle).read(seq[Arti]).map(apply)
  )

  def toJson(arti:ArtiSet):String = {
    implicit val fmts = DefaultFormats
    import org.json4s.jackson.Serialization.write
    write(arti)
  }
}