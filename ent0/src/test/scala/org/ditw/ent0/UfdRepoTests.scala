package org.ditw.ent0
import org.ditw.exutil1.naen.NaEn
import org.ditw.exutil1.naenRepo.NaEnRepo
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class UfdRepoTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private def loadRepo = UfdEnt.ufdRepoFromRes("/isni-ufd-test.json")
  private val origRepo = loadRepo

  private val en1 = NaEn.fromJson(
    """
      |{
      |  "neid" : 4504699139124178,
      |  "name" : "Indiana University-Purdue University Indianapolis School of Public and Environmental Affairs",
      |  "aliases" : [ "SPEA" ],
      |  "gnid" : 4259418,
      |  "exAttrs" : {
      |    "CC" : "US",
      |    "isni" : "0000000121547644",
      |    "post_code" : "46202-5199"
      |  }
      |}
    """.stripMargin)
  private val en2 = NaEn.fromJson(
    """
      |{
      |  "neid" : 4503943224944612,
      |  "name" : "UCLA Institute of the Environment",
      |  "aliases" : [ ],
      |  "gnid" : 5368361,
      |  "exAttrs" : {
      |    "CC" : "US",
      |    "isni" : "0000000090890895",
      |    "post_code" : "90095-1496"
      |  }
      |}
    """.stripMargin)
  private val en3 = NaEn.fromJson(
    """
      |{
      |  "neid" : 4503943224944618,
      |  "name" : "UCLA School of Dentistry",
      |  "aliases" : [ ],
      |  "gnid" : 5368361,
      |  "exAttrs" : {
      |    "CC" : "US",
      |    "isni" : "0000000406164516",
      |    "post_code" : "90095-1668"
      |  }
      |}
    """.stripMargin)
  private val en4 = NaEn.fromJson(
    """
      |{
      |  "neid" : 4504974017101819,
      |  "name" : "University of Massachusetts System",
      |  "aliases" : [ "UMASS" ],
      |  "gnid" : 4930956,
      |  "exAttrs" : {
      |    "CC" : "US",
      |    "isni" : "0000000121849220",
      |    "post_code" : "02125-3393"
      |  }
      |}
    """.stripMargin)

  private val testData = Table(
    ( "newEnts", "expDiff" ),
    (
      Iterable(
        en4
      ),
      Set[Long]() -> Set(0x10014000010000L)
    ),
    (
      Iterable(
        en1
      ),
      Set[Long]() -> Set(4504699139124142L+1)
    ),
    (
      Iterable(
        en1, en2, en3, en4
      ),
      Set[Long]() -> Set(4504699139124142L+1, 4503943224944614L+1, 4503943224944614L+2, 0x10014000010000L)
    )
  )

  "Adding new entity tests" should "pass" in {
    forAll (testData) { (newEnts, expDiff) =>
      val repo = loadRepo

      newEnts.foreach(repo.add)

      val diff = NaEnRepo.diff(origRepo, repo)
      diff shouldBe expDiff
    }
  }
}
