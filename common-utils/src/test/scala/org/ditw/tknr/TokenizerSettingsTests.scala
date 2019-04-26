package org.ditw.tknr
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class TokenizerSettingsTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {
  import TestHelpers._

  private val specialTokenTestData = Table(
    ("specialTokens", "inStr", "expTokens"),
//    (
//      Set("A/"),
//      " token1 A/T token2 a/",
//      IndexedSeq("token1", "A", "/", "T", "token2", "a/")
//    ),
    (
      Set("a/b", "/b"),
      " token1 /b x/b token2 a/b token3 /b a/b",
      IndexedSeq("token1", "/b", "x", "/", "b", "token2", "a/b", "token3", "/b", "a/b")
    ),
    (
      Set("a/b", "/b"),
      " token1 /b x/b token2 a/b token3 /b",
      IndexedSeq("token1", "/b", "x", "/", "b", "token2", "a/b", "token3", "/b")
    ),
    (
      Set("a/b", "/b"),
      " token1 /b x/b token2 a/b token3",
      IndexedSeq("token1", "/b", "x", "/", "b", "token2", "a/b", "token3")
    ),
    (
      Set("a/b", "/b"),
      " token1 /b token2 a/b token3",
      IndexedSeq("token1", "/b", "token2", "a/b", "token3")
    ),
    (
      Set("A/"),
      " token1 A/T token2",
      IndexedSeq("token1", "A", "/", "T", "token2")
    ),
    (
      Set("A/"),
      "A/T token2 a/",
      IndexedSeq("A", "/", "T", "token2", "a/")
    ),
    (
      Set("A/"),
      "A/T A/ token2 a/",
      IndexedSeq("A", "/", "T", "A/", "token2", "a/")
    ),
    (
      Set("A/"),
      "token2 a/",
      IndexedSeq("token2", "a/")
    ),
    (
      Set("A/"),
      "token2 a/ token3 a /",
      IndexedSeq("token2", "a/", "token3", "a", "/")
    ),
    (
      Set("A/T"),
      " token1 A/T token2",
      IndexedSeq("token1", "A/T", "token2")
    ),
    (
      Set("A/T"),
      "A/T token2",
      IndexedSeq("A/T", "token2")
    ),
    (
      Set("A/T"),
      "token2 A/T ",
      IndexedSeq("token2", "A/T")
    ),
    (
      EmptySpecialTokens,
      "A/T token2",
      IndexedSeq("A", "/", "T", "token2")
    )
  )

  "Special Tokens tests" should "pass" in {
    forAll(specialTokenTestData) { (specialTokens, inStr, expTokens) =>
      val tkr = _testTokenizer(specialTokens)
      val res = tkr.run(inStr, dict)
      val firstLine = res.linesOfTokens.head.origTokenStrs
      firstLine shouldBe expTokens
    }
  }
}
