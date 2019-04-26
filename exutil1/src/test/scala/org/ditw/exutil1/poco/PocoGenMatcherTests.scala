package org.ditw.exutil1.poco
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.matcher.{MatchPool, MatcherMgr, TCompMatcher}
import org.ditw.tknr.TknrHelpers.TknrTextSeg
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class PocoGenMatcherTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import PocoData._
  private val testData = Table(
    ("cc", "inStr", "expResult"),
    (
      CC_GB, "DND5 1PT", false
    ),
    (
      CC_GB, "DN55 1PT", true
    ),
    (
      CC_GB, "DN55 P1T", false
    ),
    (
      CC_GB, "CR2 6XH", true
    ),
    (
      CC_GB, "B33 8TH", true
    ),
    (
      CC_GB, "M1 1AE", true
    ),
    (
      CC_GB, "W1A 0AX", true
    ),
    (
      CC_GB, "EC1A 1BB", true
    ),
    (
      CC_GB, "ECVA 1BB", false
    )
  )

  private def mmgrFrom(cm: TCompMatcher) = new MatcherMgr(
    List(), List(), List(cm), List()
  )

  import org.ditw.exutil1.TestHelpers._

  "GB poco matchers" should "work" in {
    forAll (testData) { (cc, inStr, expResult) =>
      val cm = cc2Poco(cc).genMatcher
      val mmgr = mmgrFrom(cm)
      val mp = MatchPool.fromStr(inStr, testTokenizer, testDict)
      mmgr.run(mp)
      val tag = PocoTags.pocoTag(cc)
      val m = mp.get(tag)
      m.nonEmpty shouldBe expResult
    }
  }

}
