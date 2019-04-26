package org.ditw.tknr

import org.ditw.tknr.Trimmers.TrimResult
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by dev on 2018-10-26.
  */
class TrimmersTests
    extends FlatSpec
    with Matchers
    with TableDrivenPropertyChecks {
  private val trimByCommaColon = Trimmers.byChars(Set(',', ';', ':'))
  private val testData = Table(
    ("trimmer", "input", "expResult"),
    (
      trimByCommaColon,
      "test,",
      TrimResult("test", "", ",")
    ),
    (
      trimByCommaColon,
      "test",
      TrimResult("test", "", "")
    ),
    (
      trimByCommaColon,
      ":test,",
      TrimResult("test", ":", ",")
    ),
    (
      trimByCommaColon,
      ":;,;test,",
      TrimResult("test", ":;,;", ",")
    ),
    (
      trimByCommaColon,
      ":test",
      TrimResult("test", ":", "")
    )
  )

  "trimmer tests" should "pass" in {
    forAll(testData) { (trimmer, input, expResult) =>
      val res = trimmer.run(input)
      res shouldBe expResult
    }
  }
}
