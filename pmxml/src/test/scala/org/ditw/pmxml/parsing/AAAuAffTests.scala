package org.ditw.pmxml.parsing

import com.lucidchart.open.xtract.XmlReader
import org.ditw.pmxml.model._
import org.ditw.pmxml.parsing.TestHelpers.TestStr_AbstractEsc
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.xml.XML

class AAAuAffTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("inXml", "expAuAff"),
    (
      """            <AuthorList CompleteYN="Y">
        |                <Author ValidYN="Y">
        |                    <LastName>Hilaire</LastName>
        |                    <ForeName>F</ForeName>
        |                    <Initials>F</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                   <CollectiveName>TRUST Investigators</CollectiveName>
        |                </Author>
        |                <Author ValidYN="Y">
        |                   <LastName>Epstein</LastName>
        |                   <ForeName>Andrew E</ForeName>
        |                   <Initials>AE</Initials>
        |                   <AffiliationInfo>
        |                     <Affiliation>Department of Cardiology, University of Pennsylvania, PA, USA.</Affiliation>
        |                   </AffiliationInfo>
        |                 </Author>
        |             </AuthorList>
      """.stripMargin,
      AAAuAff(
        Seq(
          AAAuSingle(true, 0, "Hilaire", "F", Option("F"), Seq(1), None),
          AAAuSingle(true, 2, "Epstein", "Andrew E", Option("AE"), Seq(0), None)
        ),
        Seq(
          AAAuColl("TRUST Investigators", 1)
        ),
        Seq(
          AAAff(
            0,
            AffInfo("Department of Cardiology, University of Pennsylvania, PA, USA.")
          ),
          AAAff(
            1,
            AffInfo("UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.")
          )

        )
      )
    ),
    (
      """            <AuthorList CompleteYN="Y">
        |                <Author ValidYN="Y">
        |                    <LastName>Hilaire</LastName>
        |                    <ForeName>F</ForeName>
        |                    <Initials>F</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |             </AuthorList>
      """.stripMargin,
      AAAuAff(
        Seq(AAAuSingle(true, 0, "Hilaire", "F", Option("F"), Seq(0), None)),
        Seq(),
        Seq(
          AAAff(
            0,
            AffInfo("UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.")
          )
        )
      )
    ),
    (
      """            <AuthorList CompleteYN="Y">
        |                <Author ValidYN="Y">
        |                    <LastName>Hilaire</LastName>
        |                    <ForeName>F</ForeName>
        |                    <Initials>F</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                    <LastName>Basset</LastName>
        |                    <ForeName>E</ForeName>
        |                    <Initials>E</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>ENGIE, Research and Technologies Division, CRIGEN, 361 av. du Président Wilson, BP 33, 93211 St-Denis-la-Plaine Cedex, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                    <LastName>Bayard</LastName>
        |                    <ForeName>R</ForeName>
        |                    <Initials>R</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>Univ Lyon, INSA-Lyon, DEEP - EA 7429, 9 rue de la Physique, F69621 Villeurbanne Cedex, France; RECORD - Campus LyonTech, 66 bld. Niels Bohr CEI 1-CS52132, Villeurbanne Cedex, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                    <LastName>Gallardo</LastName>
        |                    <ForeName>M</ForeName>
        |                    <Initials>M</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>ENGIE, Research and Technologies Division, CRIGEN, 361 av. du Président Wilson, BP 33, 93211 St-Denis-la-Plaine Cedex, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                    <LastName>Thiebaut</LastName>
        |                    <ForeName>D</ForeName>
        |                    <Initials>D</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France. Electronic address: didier.thiebaut@espci.fr.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |                <Author ValidYN="Y">
        |                    <LastName>Vial</LastName>
        |                    <ForeName>J</ForeName>
        |                    <Initials>J</Initials>
        |                    <AffiliationInfo>
        |                        <Affiliation>UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.</Affiliation>
        |                    </AffiliationInfo>
        |                </Author>
        |            </AuthorList>
      """.stripMargin,
      AAAuAff(
        Seq(
          AAAuSingle(true, 0, "Hilaire", "F", Option("F"), Seq(1), None),
          AAAuSingle(true, 1, "Basset", "E", Option("E"), Seq(0), None),
          AAAuSingle(true, 2, "Bayard", "R", Option("R"), Seq(3), None),
          AAAuSingle(true, 3, "Gallardo", "M", Option("M"), Seq(0), None),
          AAAuSingle(true, 4, "Thiebaut", "D", Option("D"), Seq(2), None),
          AAAuSingle(true, 5, "Vial", "J", Option("J"), Seq(1), None)
        ),
        Seq(),
        Seq(
          AAAff(
            0,
            AffInfo("ENGIE, Research and Technologies Division, CRIGEN, 361 av. du Président Wilson, BP 33, 93211 St-Denis-la-Plaine Cedex, France.")
          ),
          AAAff(
            1,
            AffInfo("UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France.")
          ),
          AAAff(
            2,
            AffInfo("UMR CBI, Laboratoire des Sciences Analytiques, Bioanalytiques et Miniaturisation, ESPCI Paris, PSL Research University, 10 rue Vauquelin, 75231 Paris Cedex 05, France. Electronic address: didier.thiebaut@espci.fr.")
          ),
          AAAff(
            3,
            AffInfo("Univ Lyon, INSA-Lyon, DEEP - EA 7429, 9 rue de la Physique, F69621 Villeurbanne Cedex, France; RECORD - Campus LyonTech, 66 bld. Niels Bohr CEI 1-CS52132, Villeurbanne Cedex, France.")
          )
        )
      )
    )
  )

  "AAAuAff tests" should "pass" in {
    forAll(testData) { (inXml, expAuAff) =>
      val xml = XML.loadString(inXml)
      val parsed = XmlReader.of[AuthorList].read(xml)
      parsed.errors.nonEmpty shouldBe false

      val auAff = parsed.map(AAAuAff.createFrom).getOrElse(AAAuAff.EmptyAuAff)

      auAff shouldBe expAuAff
    }
  }
}
