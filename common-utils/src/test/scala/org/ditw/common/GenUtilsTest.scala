package org.ditw.common
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class GenUtilsTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("inStr", "res"),
    (
      "ba--",
      Vector("ba", "-", "-")
    ),
    (
      "ba-",
      Vector("ba", "-")
    ),
    (
      "a-b",
      Vector("a", "-", "b")
    ),
    (
      "-ba",
      Vector("-", "ba")
    ),
    (
      "-b a",
      Vector("-", "b a")
    ),
    (
      "--b a",
      Vector("-", "-", "b a")
    ),
    (
      "a---b",
      Vector("a", "-", "-", "-", "b")
    )
  )

  "split preserve test" should "pass" in {
    forAll(testData) { (inStr, exp) =>
      val res = GenUtils.splitPreserve(inStr, '-')
      res shouldBe exp
    }
  }

}
