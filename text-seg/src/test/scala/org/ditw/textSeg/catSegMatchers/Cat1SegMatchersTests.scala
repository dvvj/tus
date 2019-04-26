package org.ditw.textSeg.catSegMatchers
import org.ditw.common.TkRange
import org.ditw.matcher.MatchPool
import org.ditw.textSeg.TestHelpers
import org.ditw.tknr.TknrHelpers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class Cat1SegMatchersTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  import org.ditw.textSeg.common.AllCatMatchers._
  import org.ditw.textSeg.common.Tags._
  import org.ditw.textSeg.common.Vocabs
  private val corpTestData = Table(
    ("inStr", "tag", "expRanges"),
    (
      "Sth Else, Integrated Laboratory Systems, #Inc., Sth Else, Sth.",
      TagGroup4Corp.segTag,
      Set(
        (0, 2, 6)
      )
    ),
    (
      "#Inc., Sth Else, Sth.",
      TagGroup4Corp.segTag,
      Set[(Int, Int, Int)]()
    ),
    (
      "Sth Else #Integrated Laboratory Systems, Inc., Sth Else, Sth.",
      TagGroup4Corp.segTag,
      Set(
        (0, 2, 6)
      )
    ),
    (
      "Sth Else, Integrated Laboratory Systems, Inc., Sth Else, Sth.",
      TagGroup4Corp.segTag,
      Set(
        (0, 2, 6)
      )
    ),
    (
      "Integrated Laboratory Systems, Inc.,",
      TagGroup4Corp.segTag,
      Set(
        (0, 0, 4)
      )
    ),
    (
      "Sth Else, Integrated Laboratory Systems, Inc.,",
      TagGroup4Corp.segTag,
      Set(
        (0, 2, 6)
      )
    ),
    (
      "Integrated Laboratory Systems Inc.,",
      TagGroup4Corp.segTag,
      Set(
        (0, 0, 4)
      )
    )
  )

  private val dict = TestVocabs.AllVocabDict

  private val mmgr = mmgrFrom(
    dict,
    Cat1SegMatchers.segMatchers(dict),
    Cat2SegMatchers.segMatchers(dict)
  )
  "Corp SegMatcher tests" should "pass" in {
    forAll(corpTestData) { (inStr, tag, expRanges) =>
      TestHelpers.runAndVerifyRanges(
        TestVocabs.AllVocabDict,
        mmgr, inStr, tag, expRanges
      )
    }
  }
}
