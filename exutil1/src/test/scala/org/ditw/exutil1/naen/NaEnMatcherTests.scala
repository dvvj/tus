package org.ditw.exutil1.naen
import org.ditw.exutil1.TestHelpers
import org.ditw.matcher.MatchPool
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class NaEnMatcherTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("inStr", "expNEIds"),
    (
      "aurora behavioral healthcare",
      Set(2000000268L, 2000000269L, 2000000270L)
    ),
    (
      "UNIVERSITY OF DELAWARE",
      Set(1000006652L)
    )
  )

  import TestHelpers._
  "NaEn matcher tests" should "pass" in {
    forAll(testData) { (inStr, expNEIds) =>
      val mp = MatchPool.fromStr(inStr, testTokenizer, testDict)
      mmgr.run(mp)
      println(mp)
    }
  }
}
