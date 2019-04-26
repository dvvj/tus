package org.ditw.tknr
import org.ditw.matcher.MatchPool
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class SeqOfTokensTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import TestHelpers._
  import TknrHelpers._

  private val testTokens1 = IndexedSeq(
    noPfxSfx("Cardiovascular"),
    noPfx("Research", ","),
    noPfxSfx("Vrije"),
    noPfx("University", ","),
    noPfxSfx("Amsterdam")
  )
  private val constructorTestData = Table(
    ("tokenContents"),
    (
      testTokens1
    )
  )

  "constructor" should "work" in {
    forAll(constructorTestData) { (tokenContents) =>
      val lot = loTFrom(tokenContents)
      lot.length shouldBe tokenContents.length
    }
  }

  private val filterTestData = Table(
    ("tokenContents", "filteredCount"),
    (
      testTokens1, 2
    )
  )

  "filter" should "work" in {
    forAll(filterTestData) { (tokenContents, filteredCount) =>
      val lot = loTFrom(tokenContents)
      val lot2 = lot.filter(!_.sfx.isEmpty)
      lot2.length shouldBe filteredCount
      lot2.isInstanceOf[SeqOfTokens] shouldBe true
    }
  }

  private val rangeBySfxsTestData = Table(
    ("inStr", "range0", "rangeBySfxCounts", "expRngs"),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (3, 2),
      (0, 0, 6)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (2, 2),
      (0, 0, 6)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (1, 4),
      (0, 1, 7)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (1, 3),
      (0, 1, 7)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (1, 2),
      (0, 1, 6)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      SeqOfTokens.RangeBySfxCountsDefault,
      (0, 1, 3)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (2, 1),
      (0, 0, 3)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (3, 1),
      (0, 0, 3)
    ),
    (
      "1, 3 2 , 2 4, 1",
      (0, 2, 3),
      (4, 1),
      (0, 0, 3)
    )
  )

  "rangeBySfxs tests" should "pass" in {
    forAll(rangeBySfxsTestData) { (inStr, range0, rangeBySfxCounts, expRng) =>
      val mp = MatchPool.fromStr(
        inStr, testTokenizer, dict
      )
      val r0 = rangeFromTp3(mp.input, range0)
      val res = mp.input.linesOfTokens(0).rangeBySfxs(
        r0, Set(","), rangeBySfxCounts
      )
      val exp = rangeFromTp3(mp.input, expRng)
      res shouldBe exp
    }

  }

  private val rangeByPfxsTestData = Table(
    ("inStr", "range0", "rangeByPfxCounts", "expRngs"),
    (
      "1, #3 2 # 2 4 #1",
      (0, 2, 3),
      (3, 2),
      (0, 0, 6)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (2, 2),
      (0, 0, 6)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (1, 4),
      (0, 1, 7)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (1, 3),
      (0, 1, 7)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (1, 2),
      (0, 1, 6)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      SeqOfTokens.RangeBySfxCountsDefault,
      (0, 1, 3)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (2, 1),
      (0, 0, 3)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (3, 1),
      (0, 0, 3)
    ),
    (
      "1 #3 2 # 2 4 #1",
      (0, 2, 3),
      (4, 1),
      (0, 0, 3)
    )
  )


  "rangeByPfxs tests" should "pass" in {
    forAll(rangeByPfxsTestData) { (inStr, range0, rangeBySfxCounts, expRng) =>
      val mp = MatchPool.fromStr(
        inStr, testTokenizer, dict
      )
      val r0 = rangeFromTp3(mp.input, range0)
      val res = mp.input.linesOfTokens(0).rangeByPfxs(
        r0, Set("#"), rangeBySfxCounts
      )
      val exp = rangeFromTp3(mp.input, expRng)
      res shouldBe exp
    }

  }
}
