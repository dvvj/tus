package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model.ArtiSet
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

class TestAbstractEsc extends FlatSpec with Matchers {

  import TestHelpers._
  ignore should "be recognized" in {
    val xml = XML.loadString(TestStr_AbstractEsc)
    val parsed = XmlReader.of[ArtiSet].read(xml)

    val abs:String = parsed.map(_.artis.head.citation._abstract.texts.head.text).getOrElse(None).asInstanceOf[String]
    abs shouldBe "... (μgNm<sup>-3</sup>) coupled with ..."
    //abs shouldBe "... (μgNm-3) coupled with ..."
  }

}
