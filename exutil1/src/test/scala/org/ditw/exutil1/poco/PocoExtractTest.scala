package org.ditw.exutil1.poco
import org.ditw.extract.XtrMgr
import org.ditw.exutil1.TestHelpers.{testDict, testTokenizer}
import org.ditw.exutil1.extract.PocoXtrs
import org.ditw.exutil1.poco.PocoData._
import org.ditw.matcher.{MatchPool, MatcherMgr, TCompMatcher}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks

class PocoExtractTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  private val testData = Table(
    ("cc", "inStr", "expRes"),
    (
      CC_US,
      "TX 77030, USA",
      List(4699066)
    ),
    (
      CC_US,
      "WA 98109, USA",
      List(5809844)
    ),
    (
      CC_US,
      "WA 98109-5234, USA",
      List(5809844)
    ),
    (
      CC_US,
      "WA 98109‑5234, USA",
      List(5809844)
    ),
    (
      CC_GB,
      "Bath BA2 7AY, UK.",
      List(2656173)
    ),
    (
      CC_GB,
      "Sheffield, S10 1RX, UK",
      List(3333193)
    ),
    (
      CC_GB,
      "Sheffield, S3 7HF, UK.",
      List(2638077)
    ),
    (
      CC_GB,
      "Norwich, NR4 7UA, UK.",
      List(2641181)
    ),
    (
      CC_GB,
      "Cardiff University, Cardiff, CF10 3NB, UK.",
      List(2653822)
    ),
    (
      CC_GB,
      "Granta Park, Cambridge CB21 6GH, UK.",
      List(2653941)
    ),
    (
      CC_GB,
      "Lowestoft NR33 0HT, UK.",
      List(2643490)
    )
  )

  private def mmgrFrom(cm: TCompMatcher) = new MatcherMgr(
    List(), List(), List(cm), List()
  )
  import PocoXtrs._
  private val xtrMgr = XtrMgr.create(cc2Poco.values.map(_.xtr).toList)

  "Poco extract tests" should "pass" in {
    forAll (testData) { (cc, inStr, expRes) =>
      val cm = cc2Poco(cc).genMatcher
      val mmgr = mmgrFrom(cm)
      val mp = MatchPool.fromStr(inStr, testTokenizer, testDict)
      mmgr.run(mp)
      val ids = xtrMgr.run(mp)
      ids.values.flatten.toList shouldBe expRes
    }
  }
}
