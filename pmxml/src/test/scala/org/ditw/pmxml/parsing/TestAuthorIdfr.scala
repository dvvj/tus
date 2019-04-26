package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model.{AffInfo, ArtiSet, Author, Identifier}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

class TestAuthorIdfr extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import TestHelpers._
  private val testData = Table(
    ("testxml", "authorsWithIdentifier"),
    (
      TestStr_AuthorWithIdentifier,
      Map(
        28993670 -> Seq(
          Author.fullName(
            "Y", "Palesi", "Fulvia", "F",
            Seq(
              AffInfo("Department of Physics, University of Pavia, Pavia, PV, Italy. fulvia.palesi@unipv.it."),
              AffInfo("Brain Connectivity Center, C. Mondino National Neurological Institute, Pavia, PV, Italy. fulvia.palesi@unipv.it.")
            ),
            Option(Identifier("ORCID", "http://orcid.org/0000-0001-5027-8770"))
          )
        ),
        28993301 -> Seq(
          Author.fullName(
            "Y", "Mizdrak", "Anja", "A",
            Seq(
              AffInfo("Burden of Disease Epidemiology, Equity and Cost-Effectiveness Programme (BODE3), Department of Public Health, University of Otago, Wellington, Wellington, New Zealand.")
            ),
            Option(Identifier("ORCID", "http://orcid.org/0000-0002-2897-3002"))
          ),
          Author.fullName(
            "Y", "Waterlander", "Wilma Elzeline", "WE",
            Seq(
              AffInfo("National Institute of Health Innovation, University of Auckland, Auckland, New Zealand.")
            ),
            Option(Identifier("ORCID", "http://orcid.org/0000-0003-0956-178X"))
          ),
          Author.fullName(
            "Y", "Rayner", "Mike", "M",
            Seq(
              AffInfo("Centre on Population Approaches for Non-Communicable Disease Prevention, Nuffield Department of Population Health, University of Oxford, Oxford, United Kingdom.")
            ),
            Option(Identifier("ORCID", "http://orcid.org/0000-0003-0479-6483"))
          ),
          Author.fullName(
            "Y", "Scarborough", "Peter", "P",
            Seq(
              AffInfo("Centre on Population Approaches for Non-Communicable Disease Prevention, Nuffield Department of Population Health, University of Oxford, Oxford, United Kingdom.")
            ),
            Option(Identifier("ORCID", "http://orcid.org/0000-0002-2378-2944"))
          )
        )
      )
    )
  )

  "Author with identifier tests" should "pass" in {
    forAll (testData) { (textxml, authorsWithId) =>
      val xml = XML.loadString(textxml)
      val parsed = XmlReader.of[ArtiSet].read(xml)

      val pmid2AuthorsWithIds = parsed.map { p =>
        p.artis.map(arti => arti.citation.pmid -> arti.citation.authors.authors.filter(_.idfr.nonEmpty)).toMap
      }.getOrElse(Map())
      println(pmid2AuthorsWithIds.size)

      pmid2AuthorsWithIds shouldBe authorsWithId
    }
  }
}
