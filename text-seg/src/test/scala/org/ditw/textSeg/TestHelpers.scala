package org.ditw.textSeg
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.matcher.{MatchPool, MatcherMgr}
import org.ditw.textSeg.common.Vocabs
import org.ditw.tknr.TknrHelpers.{TknrTextSeg, loTFrom, resultFrom}
import org.ditw.tknr.{TknrHelpers, TknrResult}
import org.scalatest.Matchers

object TestHelpers extends Matchers {

  import TknrHelpers._
  private[textSeg] val _Dict: Dict =
    InputHelpers.loadDict(
      "0123456789".map(_.toString)
    )

  private [textSeg] def testDataTuple(
    testStr:String,
    testContent:IndexedSeq[IndexedSeq[String]]
  ):(String, TknrResult) = {
    testStr ->
      resultFrom(
        testStr,
        _Dict,
        IndexedSeq(loTFrom(testContent))
      )
  }

  val testTokenizer = TknrTextSeg()

  def runAndVerifyRanges(
    dict: Dict,
    mmgr:MatcherMgr,
    inStr:String, tag:String, expRanges:Set[(Int, Int, Int)]
  ):Unit = {
    val matchPool = MatchPool.fromStr(inStr, testTokenizer, dict)
    mmgr.run(matchPool)
    val res = matchPool.get(tag)
    val resRanges = res.map(_.range)
    val expRngs = expRanges.map(tp => TknrHelpers.rangeFromTp3(matchPool.input, tp))
    resRanges shouldBe expRngs
  }
}
