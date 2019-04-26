package org.ditw.common
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class DictTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val loadDictTestData = Table(
    ("words", "wordsInDict"),
    (
      Set(
        "word", "WORD", "w2", "w3"
      ),
      Set(
        "word", "w2", "w3"
      )
    ),
    (
      Set(
        "WOrd", "WORD", "w2", "w3"
      ),
      Set(
        "word", "w2", "w3"
      )
    )
  )
  import InputHelpers._
  "loadDict test" should "pass" in {
    forAll (loadDictTestData) { (words, wordsInDict) =>
      val dict = loadDict(words)
      dict.size shouldBe wordsInDict.size
      wordsInDict.foreach { w => dict.contains(w) shouldBe true }
    }
  }

  private val dictEncTestData = Table(
    ("words", "encMap"),
    (
      Seq(
        Set(
          "word", "WORD", "w2", "w3"
        )
      ),
      Map(
        "word" -> 0,
        "w2" -> 1,
        "w3" -> 2
      )
    ),
    (
      Seq(
        Set(
          "word", "WORD", "w2", "w3"
        ),
        Set(
          "w4", "wORD", "w3"
        )
      ),
      Map(
        "word" -> 0,
        "w2" -> 1,
        "w3" -> 2,
        "w4" -> 3
      )
    )
  )

  "Dict::enc/dec test" should "pass" in {
    forAll (dictEncTestData) { (words, encMap) =>
      val dict = loadDict0(words)
      dict.size shouldBe encMap.size
      encMap.keySet.foreach { k =>
        dict.enc(k) shouldBe encMap(k)
        val v = encMap(k)
        dict.dec(v) shouldBe k
      }
    }
  }

  import Dict._
  private val dictEncTestData2 = Table(
    ("words", "testWords", "encoded"),
    (
      Seq(
        Set(
          "word", "WORD", "w2", "w3"
        )
      ),
      IndexedSeq(
        "word", "WORD", "w2", "w3"
      ),
      IndexedSeq(
        0, 0, 1, 2
      )
    ),
    (
      Seq(
        Set(
          "word", "WORD", "w2", "w3"
        )
      ),
      IndexedSeq(
        "word1", "WORD", "w22", "w3"
      ),
      IndexedSeq(
        UNKNOWN, 0, UNKNOWN, 2
      )
    )
  )

  "Dict::enc" should "return UNKNOWN if word not found" in {
    forAll (dictEncTestData2) { (words, testWords, encoded) =>
      val dict = loadDict0(words)
      val res = testWords.map(dict.enc)
      res shouldBe encoded
    }
  }
}
