package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model.{AffInfo, ArtiSet, Author, Identifier}
import org.ditw.pmxml.parsing.TestHelpers._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

class TestAffIdfr extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("testXml", "authorWithAffsWithIdfr"),
    (
      TestStr_AffWithIdfr,
      Map(
        29151804 -> Seq(
          Author.fullName(
            "Y", "Dixit", "Shalabh", "S",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              )
            )
          ),
          Author.fullName(
            "Y", "Singh", "Anshuman", "A",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              ),
              AffInfo(
                "Rani Lakshmi Bai Central Agriculture University, Jhansi, India."
              )
            )
          ),
          Author.fullName(
            "Y", "Sandhu", "Nitika", "N",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              )
            )
          ),
          Author.fullName(
            "Y", "Bhandari", "Aditi", "A",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              )
            )
          ),
          Author.fullName(
            "Y", "Vikram", "Prashant", "P",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              ),
              AffInfo(
                "International Maize and Wheat Improvement Center (CIMMYT), Km. 45, Carretera México-Veracruz, El Batán, 56237 Texcoco, CP Mexico.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 2289 885X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.433436.5"
                  )
                )
              )
            )
          ),
          Author.fullName(
            "Y", "Kumar", "Arvind", "A",
            Seq(
              AffInfo(
                "International Rice Research Institute (IRRI), DAPO Box 7777, Metro Manila, Philippines.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0729 330X"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.419387.0"
                  )
                )
              )
            )
          )
        ),
        29151805 -> Seq(
          Author.fullName(
            "Y", "Fiala", "Lenka", "L",
            Seq(
              AffInfo(
                "CentER, TILEC, Tilburg University, Tilburg, The Netherlands.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0943 3265"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.12295.3d"
                  )
                )
              )
            )
          ),
          Author.fullName(
            "Y", "Suetens", "Sigrid", "S",
            Seq(
              AffInfo(
                "CentER, TILEC, Tilburg University, Tilburg, The Netherlands.",
                Seq(
                  Identifier(
                    src="ISNI",
                    v="0000 0001 0943 3265"
                  ),
                  Identifier(
                    src="GRID",
                    v="grid.12295.3d"
                  )
                )
              )
            ),
            Option(Identifier("ORCID", "0000-0001-6168-6059"))
          )
        )
      )
    )
  )


  "affInfo tests" should "pass" in {
    forAll(testData) { (testXml, authorWithAffsWithIdfr) =>
      val xml = XML.loadString(testXml)
      val parsed = XmlReader.of[ArtiSet].read(xml)
      parsed.errors.isEmpty shouldBe true

      val pmid2Authors = parsed.map { p =>
        p.artis.map(arti =>
          arti.citation.pmid -> arti.citation.authors.authors.filter(
            au => au.affInfo.exists(_.idfrs.nonEmpty)
          )
        ).toMap
      }.getOrElse(Map())

      pmid2Authors shouldBe authorWithAffsWithIdfr
    }
  }
}
