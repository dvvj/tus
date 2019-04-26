package org.ditw.tknr

import org.ditw.common.{Dict, InputHelpers}
import org.ditw.tknr.Tokenizers.TokenizerSettings
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by dev on 2018-10-26.
  */
class TknrResultsTests
    extends FlatSpec
    with Matchers
    with TableDrivenPropertyChecks {

  import TestHelpers._
  import TknrHelpers._

  private val testStr1 = "Cardiovascular Research, Vrije University, Amsterdam"
  private val tokenContent1 = IndexedSeq(
    IndexedSeq(
      noPfxSfx("Cardiovascular"),
      commaSfx("Research"),
      noPfxSfx("Vrije"),
      commaSfx("University"),
      noPfxSfx("Amsterdam")
    )

  )
  private val testStr2 = "Cardiovascular Research,\nVrije University, Amsterdam"
  private val tokenContent2 = IndexedSeq(
    IndexedSeq(
      noPfxSfx("Cardiovascular"),
      commaSfx("Research")
    ),
    IndexedSeq(
      noPfxSfx("Vrije"),
      commaSfx("University"),
      noPfxSfx("Amsterdam")
    )
  )
  private val testStr3 =
    "Cardiovascular Research,\n Vrije University, Amsterdam"
  private val tokenContent3 = IndexedSeq(
    IndexedSeq(
      noPfxSfx("Cardiovascular"),
      commaSfx("Research")
    ),
    IndexedSeq(
      noPfxSfx("Vrije"),
      commaSfx("University"),
      noPfxSfx("Amsterdam")
    )
  )
  private val testStr4 =
    "\"Angelo Nocivelli\" Institute for Molecular Medicine"
  private val tokenContent4 = IndexedSeq(
    IndexedSeq(
      noSfx("Angelo", "\""),
      noPfx("Nocivelli", "\""),
      noPfxSfx("Institute"),
      noPfxSfx("for"),
      noPfxSfx("Molecular"),
      noPfxSfx("Medicine")
    )
  )

  private val testStr5 =
    "(Formerly) Department of Anthropology"
  private val tokenContent5 = IndexedSeq(
    IndexedSeq(
      IndexedSeq("Formerly", "(", ")"),
      noPfxSfx("Department"),
      noPfxSfx("of"),
      noPfxSfx("Anthropology")
    )
  )

  private val testStr6 =
    "*Auburn University (Professor Emeritus), Auburn, AL;"
  private val tokenContent6 = IndexedSeq(
    IndexedSeq(
      noSfx("Auburn", "*"),
      noPfxSfx("University"),
      noSfx("Professor", "("),
      noPfx("Emeritus", "),"),
      noPfx("Auburn", ","),
      noPfx("AL", ";")
    )
  )

  private val testStr7 =
    "*Auburn University- (Professor Emeritus), Auburn, AL;"
  private val tokenContent7 = IndexedSeq(
    IndexedSeq(
      noSfx("Auburn", "*"),
      noPfxSfx("University"),
      noPfxSfx("-"),
      noSfx("Professor", "("),
      noPfx("Emeritus", "),"),
      noPfx("Auburn", ","),
      noPfx("AL", ";")
    )
  )

  private val testStr8 = "Cardiovascular Research,Vrije University, Amsterdam"
  private val tokenContent8 = IndexedSeq(
    IndexedSeq(
      noPfxSfx("Cardiovascular"),
      commaSfx("Research"),
      noPfxSfx("Vrije"),
      commaSfx("University"),
      noPfxSfx("Amsterdam")
    )

  )

  private val testStr9 = "Cardiovascular Research;Vrije University, Amsterdam"
  private val tokenContent9 = IndexedSeq(
    IndexedSeq(
      noPfxSfx("Cardiovascular"),
      noPfx("Research", ";"),
      noPfxSfx("Vrije"),
      commaSfx("University"),
      noPfxSfx("Amsterdam")
    )

  )


  private val testData = Table(
    ("input", "expRes"),
    testDataTuple(
      testStr9,
      tokenContent9
    ),
    testDataTuple(
      testStr8,
      tokenContent8
    ),
    testDataTuple(
      testStr7,
      tokenContent7
    ),
    testDataTuple(
      testStr1,
      tokenContent1
    ),
    testDataTuple(
      testStr2,
      tokenContent2
    ),
    testDataTuple(
      testStr3,
      tokenContent3
    ),
    testDataTuple(
      testStr4,
      tokenContent4
    ),
    testDataTuple(
      testStr5,
      tokenContent5
    ),
    testDataTuple(
      testStr6,
      tokenContent6
    )
  )

  "tokenizer tests" should "pass" in {
    forAll(testData) { (input, expRes) =>
      val res = testTokenizer.run(input, dict)

//      val expLrs = expLineTokens.map()
//      val expRes = TknrResult(expLineResults)
//      res.lineResults.size shouldBe expRes.lineResults.size
      //res.linesOfTokens.size shouldBe expRes.linesOfTokens.size
      resEqual(res, expRes) shouldBe true
    }
  }
}
