package org.ditw.demo1.matchers
import org.ditw.demo1.TestData
import org.ditw.demo1.gndata.GNCntry
import org.ditw.matcher.MatchPool
import org.ditw.tknr.TknrHelpers
import org.ditw.tknr.TknrHelpers.rangeFromTp3
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class Adm1MatcherBasicTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import TagHelper._
  private val testData = Table(
    ("inStr", "expTag", "expRange"),
    (
      "City of Boston, Massachusetts, USA.",
      cityStateTag("US_MA"),
      Set(
        (0, 0, 4)
      )
    ),
    (
      "Boston, Massachusetts, USA.",
      cityStateTag("US_MA"),
      Set(
        (0, 0, 2)
      )
    ),
    (
      "Montréal (Québec), Canada",
      cityCountryTag(GNCntry.CA),
      Set[(Int, Int, Int)]() // blocked by cityState match
    ),
    (
      "Montréal (Québec), Canada",
      cityStateTag("CA_10"),
      Set(
        (0, 0, 2)
      )
    ),
    (
      "Waterloo, Ontario, Canada",
      cityStateTag("CA_08"),
      Set(
        (0, 0, 2)
      )
    ),
    (
      "Waterloo, Ontario, Canada",
      cityCountryTag(GNCntry.CA),
      Set[(Int, Int, Int)]()
    ),
//    (
//      "Washington, DC, USA.",
//      cityCountryTag(GNCntry.US),
//      Set(
//        (0, 0, 3)
//      )
//    ),
    (
      "Worcester County, Massachusetts, USA.",
      cityStateTag("US_MA"),
      Set(
        (0, 0, 3)
      )
    ),
    (
      "Boston, Massachusetts, USA.",
      cityStateTag("US_MA"),
      Set(
        (0, 0, 2)
      )
    )
  )

  "adm1 matchers" should "work" in {
    forAll(testData) { (inStr, expTag, expRanges) =>
      val mp = MatchPool.fromStr(
        inStr, MatcherHelper.testTokenizer, TestData.testDict
      )
      MatcherHelper.mmgr.run(mp)

      val ranges = mp.get(expTag).map(_.range)

      val exp = expRanges.map(rangeFromTp3(mp.input, _))

      ranges shouldBe exp
    }
  }
}
