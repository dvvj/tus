package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model.{ArtiSet, MeshHeading, MeshHeadings, MeshName}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

class TestMeshParsing extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testStr = TestHelpers.loadRes("/testdata/with-mesh.xml")

  private val testData = Table(
    ("textxml", "meshTermMaps"),
    (
      testStr,
      Map(
        28993193 -> MeshHeadings(
          Seq(
            MeshHeading(
              MeshName("D000072283", "N", "A549 Cells"),
              Seq()
            ),
            MeshHeading(
              MeshName("D049109", "N", "Cell Proliferation"),
              Seq(
                MeshName("Q000187", "N", "drug effects")
              )
            ),
            MeshHeading(
              MeshName("D056743", "N", "Cyclin D3"),
              Seq(
                MeshName("Q000235", "N", "genetics"),
                MeshName("Q000378", "N", "metabolism")
              )
            )
          )
        )
      )
    )
  )

  "mesh heading parsing test" should "pass" in {
    forAll(testData) { (textxml, meshTermMaps) =>
      val xml = XML.loadString(textxml)
      val parsed = XmlReader.of[ArtiSet].read(xml)
      parsed.isSuccessful shouldBe true

      val pmid2Arti = parsed.map { artiSet =>
        val pmid2Mesh = artiSet.artis
          .map(arti => arti.citation.pmid -> arti.citation.meshHeadings)
          .toMap
        pmid2Mesh shouldBe meshTermMaps
      }
    }
  }
}
