package org.ditw.matcher
import org.ditw.common.TkRange
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class CompMatcherNsTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import CompMatchers._
  import CompMatcherNs._
  import TokenMatchers._
  import org.ditw.tknr.TestHelpers._

  private val tag1_9 = "tag1_9"
  private val matcher1_9 = lng(
    IndexedSeq(
      byTm(
        ngram(
          Set(Array("1")), dict)
      ),
      byTm(
        ngram(
          Set(Array("9")), dict)
      )
    ),
    tag1_9
  )
  private val tag9_23 = "tag9_23"
  private val matcher9_23 = lng(
    IndexedSeq(
      byTm(
        ngram(
          Set(Array("9")), dict)
      ),
      byTm(
        ngram(
          Set(Array("2"), Array("3")), dict)
      )
    ),
    tag9_23
  )
  private val tag12_23 = "tag12_23"
  private val matcher12_23 = lng(
    IndexedSeq(
      byTm(
        ngram(
          Set(Array("1"), Array("2")), dict)
      ),
      byTm(
        ngram(
          Set(Array("2"), Array("3")), dict)
      )
    ),
    tag12_23
  )
  private val seqTestData = Table(
    ( "cms", "inStr", "expResultMap" ),
    (
      List(matcher1_9),
      "1, 2, 3 4",
      Map(
        matcher1_9.tag.get -> Set[(Int, Int, Int)]()
      )
    ),
    (
      List(matcher9_23),
      "1, 2, 3 4",
      Map(
        matcher9_23.tag.get -> Set[(Int, Int, Int)]()
      )
    ),
    (
      List(matcher12_23),
      "2, 3 4\n2\" 1 3",
      Map(
        matcher12_23.tag.get -> Set(
          (0, 0, 2), (1, 1, 3)
        )
      )
    ),
    (
      List(matcher12_23),
      "1, 2, 3 4",
      Map(
        matcher12_23.tag.get -> Set(
          (0, 0, 2), (0, 1, 3)
        )
      )
    )
  )

  import org.ditw.tknr.TknrHelpers._
  import MatcherTestsUtils._
  "seq matcher tests" should "pass" in {
    forAll(seqTestData) { (cms, inStr, expRes) =>
      val (matchPool, res2Ranges) = runCms(List(), cms, inStr, expRes.keySet)

      val expMatcheRanges = expRes.mapValues { tp =>
        tp.map(p => rangeFromTp3(matchPool.input, p))
      }
      res2Ranges shouldBe expMatcheRanges
    }
  }

}
