package org.ditw.common
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TkRangeTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import org.ditw.tknr.TestHelpers._
  private val origStrTestData = Table(
    ("inStr", "rangeCoord", "origStr"),
    (
      "Cardiovascular Research, Vrije University, Amsterdam",
      (0, 0, 2),
      "Cardiovascular Research,"
    ),
    (
      "Cardiovascular Research,\nVrije University, Amsterdam",
      (1, 0, 2),
      "Vrije University,"
    ),
    (
      "Cardiovascular Research,\nVrije University, Amsterdam",
      (1, 0, 3),
      "Vrije University, Amsterdam"
    ),
    (
      "NotInDict1 NotInDict2, Vrije University, Amsterdam",
      (0, 0, 2),
      "NotInDict1 NotInDict2,"
    )
  )

  "TkRange::origStr tests" should "pass" in {
    forAll(origStrTestData) { (inStr, coord, expOrigStr) =>
      val input = testTokenizer.run(inStr, dict)
      val range = new TkRange(input, coord._1, coord._2, coord._3)
      range.origStr shouldBe expOrigStr
    }
  }

  private val strTestData = Table(
    ("inStr", "rangeCoord", "str"),
    (
      "Cardiovascular Research, Vrije University, Amsterdam",
      (0, 0, 2),
      "Cardiovascular Research"
    ),
    (
      "Cardiovascular Research,\nVrije University, Amsterdam",
      (1, 0, 2),
      "Vrije University"
    ),
    (
      "Cardiovascular Research,\nVrije University, Amsterdam",
      (1, 0, 3),
      "Vrije University Amsterdam"
    ),
    (
      "NotInDict1 NotInDict2, Vrije University, Amsterdam",
      (0, 0, 2),
      "NotInDict1 NotInDict2"
    )
  )

  "TkRange::str tests" should "pass" in {
    forAll(strTestData) { (inStr, coord, expStr) =>
      val input = testTokenizer.run(inStr, dict)
      val range = new TkRange(input, coord._1, coord._2, coord._3)
      range.str shouldBe expStr
    }
  }

}
