package org.ditw.demo1.matchers
import org.ditw.demo1.TestData
import org.ditw.demo1.TestData.{testDict, testGNSvc}
import org.ditw.demo1.gndata.GNCntry
import org.ditw.demo1.matchers.MatcherHelper.{mmgr, xtrMgr}
import org.ditw.demo1.matchers.TagHelper.cityCountryTag
import org.ditw.matcher.MatchPool
import org.ditw.tknr.TknrHelpers
import org.ditw.tknr.TknrHelpers.rangeFromTp3
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class GNSeqMatcherTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("inStr", "expTag", "expRange"),
    (
      "Kyonancho, Musashino, Tokyo 180-8602, Japan",
      cityCountryTag(GNCntry.CA),
      Set[(Int, Int, Int)]() // blocked by cityState match
    )
  )


  "GNSeqMatcher tests" should "pass" in {
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
