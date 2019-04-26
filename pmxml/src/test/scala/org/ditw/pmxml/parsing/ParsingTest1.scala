package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model.ArtiSet
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

import TestHelpers._

object ParsingTest1 extends App {

  //val xml = XML.loadString(TestStr_Simple)
  val xml = XML.load("file:///media/sf_vmshare/_pmd/pubmed18n0923.xml")
  val parsed = XmlReader.of[ArtiSet].read(xml)
  parsed.foreach { artiSet =>
    println(artiSet.artis.size)
  }

}
