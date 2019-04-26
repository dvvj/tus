package org.ditw.textSeg
import org.ditw.common.TkRange
import org.ditw.matcher.{MatchPool, MatcherMgr, TokenMatchers}
import org.ditw.tknr.TknrHelpers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SegMatchersTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import TokenMatchers._
  import TestHelpers._
  import SegMatchers._
  import TknrHelpers._
  private val tag2 = Option("tag2")
  private val tag3 = Option("tag3")
  private val tm2 = ngram(Set(Array("2")), _Dict, tag2)
  private val tm3 = ngram(Set(Array("3")), _Dict, tag3)
  private val punctSet = Set(",", ";")
  private val tagSegBySfx = "tagSegBySfx"
  private val tagByTagsMatcherLeftOnly = "tagByTagsMatcherLeftOnly"
  private val segBySfxMatcher = segBySfx(
    Set(tag2.get),
    punctSet,
    true,
    tagSegBySfx
  )
  private val segByTagsMatcherLeftOnly = segByTags(
    segBySfxMatcher,
    tag3.toSet,
    Set(),
    tagByTagsMatcherLeftOnly
  )
  private val mmgr = new MatcherMgr(
    List(tm2, tm3),
    List(),
    List(segBySfxMatcher, segByTagsMatcherLeftOnly),
    List()
  )

  private val segBySfxTestData = Table(
    ("inStr", "expRes"),
    (
      "1, 3, 2 ,\n2 4, 1",
      Set(
        (0, 2, 3),
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 2,\n2 4, 1",
      Set(
        (0, 2, 3),
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 2\n2 4, 1",
      Set(
        (0, 2, 3),
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 4\n2 4, 1",
      Set(
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 4\n2 4",
      Set(
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 4\n1, 2 4",
      Set(
        (1, 1, 3)
      )
    ),
    (
      "1, 2 3, 4\n1, 2 4",
      Set(
        (0, 1, 3),
        (1, 1, 3)
      )
    ),
    (
      ", 2 3 , 4",
      Set(
        (0, 1, 3)
      )
    ),
    (
      ", 2 3, 4",
      Set(
        (0, 1, 3)
      )
    ),
    (
      "1, 2 3  4",
      Set(
        (0, 1, 4)
      )
    ),
    (
      "1, 2, 3, 4",
      Set(
        (0, 1, 2)
      )
    ),
    (
      "2, 3, 4",
      Set(
        (0, 0, 1)
      )
    ),
    (
      " 2, 3, 4",
      Set(
        (0, 0, 1)
      )
    ),
    (
      "1, 2 3, 4",
      Set(
        (0, 1, 3)
      )
    )
  )

  "SegBySfx Matcher tests" should "pass" in {
    forAll(segBySfxTestData) { (inStr, expRes) =>
      val matchPool = MatchPool.fromStr(inStr, testTokenizer, _Dict)
      mmgr.run(matchPool)
      val expRanges = expRes.map { tp =>
        TknrHelpers.rangeFromTp3(matchPool.input, tp)
      }
      val resRanges = matchPool.get(tagSegBySfx).map(_.range)
      resRanges shouldBe expRanges
    }
  }

  private val segByTagsTestData = Table(
    ("inStr", "segMatcher", "expRes"),
    (
      "1, 3 2 ,\n2 4, 1",
      segByTagsMatcherLeftOnly,
      Set(
        (0, 2, 3),
        (1, 0, 2)
      )
    ),
    (
      "1, 3, 2 ,\n2 4, 1",
      segByTagsMatcherLeftOnly,
      Set(
        (0, 2, 3),
        (1, 0, 2)
      )
    )
  )

  "SegByTags Matcher tests" should "pass" in {
    forAll(segByTagsTestData) { (inStr, segMatcher, expRes) =>
      val matchPool = MatchPool.fromStr(inStr, testTokenizer, _Dict)
      mmgr.run(matchPool)
      val expRanges = expRes.map { tp =>
        TkRange(matchPool.input, tp._1, tp._2, tp._3)
      }
      val resRanges = matchPool.get(segMatcher.tag.get).map(_.range)
      resRanges shouldBe expRanges
    }
  }

}
