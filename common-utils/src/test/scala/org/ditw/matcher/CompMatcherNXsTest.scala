package org.ditw.matcher
import org.ditw.matcher.CompMatcherNs.lng
import org.ditw.matcher.CompMatchers.byTm
import org.ditw.matcher.MatcherTestsUtils.runCms
import org.ditw.matcher.TokenMatchers.ngram
import org.ditw.tknr.TestHelpers.{dict, testTokenizer}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class CompMatcherNXsTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import CompMatcherNXs._
  import TokenMatchers._
  private val tag1 = "tag1"
  private val tm1 = ngramT(Set(Array("1")), dict, tag1)
  private val tag9 = "tag9"
  private val tm9 = ngramT(Set(Array("9")), dict, tag9)
  private val cmTag = "cmTag"
  private val sfx = Set(",")

  private val tms = List(tm1, tm9)

  private val testData = Table(
    ( "tms", "cms", "inStr", "expResultMap" ),
    (
      tms,
      (3, 0),
      "9, 2 1, 3 4",
      Map(
        cmTag -> Set(
          (0, 0, 3)
        )
      )
    ),
    (
      tms,
      (2, 0),
      "9, 2 1, 3 4",
      Map(
        cmTag -> Set(
          (0, 0, 3)
        )
      )
    ),
    (
      tms,
      (1, 2),
      "9, 2 1, 9, 3 4",
      Map(
        cmTag -> Set(
          (0, 2, 4)
        )
      )
    ),
    (
      tms,
      (1, 2),
      "9, 2 1, 3 9, 3 4",
      Map(
        cmTag -> Set(
          (0, 2, 5)
        )
      )
    ),
    (
      tms,
      (1, 2),
      "9, 2 1, 9 3, 3 4",
      Map(
        cmTag -> Set(
          (0, 2, 4)
        )
      )
    ),
    (
      tms,
      (1, 2),
      "9, 2 1, 9 3, 9 4",
      Map(
        cmTag -> Set(
          (0, 2, 4)
        )
      )
    ),
    (
      tms,
      (2, 0),
      "9, 1 2, 3 4",
      Map(
        cmTag -> Set(
          (0, 0, 2)
        )
      )
    ),
    (
      tms,
      (2, 0),
      "9, 2 9 1, 3 4",
      Map(
        cmTag -> Set(
          (0, 0, 4), (0, 2, 4)
        )
      )
    ),
    (
      tms,
      (1, 0),
      "2, 9 1, 3 4",
      Map(
        cmTag -> Set(
          (0, 1, 3)
        )
      )
    ),
    (
      tms,
      (2, 0),
      "9, 1, 3 4",
      Map(
        cmTag -> Set(
          (0, 0, 2)
        )
      )
    ),
    (
      tms,
      (2, 0),
      "1, 2, 3 4",
      Map(
        cmTag -> Set[(Int, Int, Int)]()
      )
    )
  )

  import org.ditw.tknr.TknrHelpers._

  "sfxLookAround matcher tests" should "pass" in {
    forAll(testData) { (tms, sfxCounts, inStr, expRes) =>
      val m = sfxLookAroundByTag_ALL(
        sfx,
        sfxCounts,
        tag1,
        tag9,
        cmTag
      )
      val (matchPool, res2Ranges) = runCms(tms, List(m), inStr, expRes.keySet)
      val expMatcheRanges = expRes.mapValues { tp =>
        tp.map(p => rangeFromTp3(matchPool.input, p))
      }
      res2Ranges shouldBe expMatcheRanges
    }
  }

}
