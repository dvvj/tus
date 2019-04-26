package org.ditw.tknr
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.tknr.Tokenizers.{TTokenizer, TokenSplitterCond, TokenizerSettings}

object TestHelpers {

  import TknrHelpers._

  val dict:Dict = InputHelpers.loadDict(
    "[\\s,]".r.pattern.split("Cardiovascular Research, Vrije University, Amsterdam")
      .filter(!_.isEmpty),
    "0123456789".map(_.toString)
  )

  //private val trimByCommaColon = Trimmers.byChars(Set(',', ';'))
  private val trimByPuncts = Trimmers.byChars(
    ",;:\"()*†#".toSet
  )

  private[tknr] val EmptySpecialTokens = Set[String]()
  private[tknr] def settings(specialTokens:Set[String]) = TokenizerSettings(
    "\\n+",
    specialTokens,
    "[\\s]+",
    DefTokenSplitters2Keep,
    List(
      TokenSplitter_CommaColon, TokenSplitter_DashSlash
    ),
    trimByPuncts
  )

  private[tknr] def _testTokenizer(specialTokens:Set[String] = EmptySpecialTokens) =
    Tokenizers.load(settings(specialTokens))
  val testTokenizer: TTokenizer = _testTokenizer()

  def testDataTuple(
    testStr:String,
    testContents:IndexedSeq[IndexedSeq[IndexedSeq[String]]]
  ):(String, TknrResult) = {
    testStr ->
      resultFrom(
        testStr,
        dict,
        testContents.map(tc => loTFrom(tc))
      )
  }

}
