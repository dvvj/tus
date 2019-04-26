package org.ditw.tknr
import org.ditw.common.{Dict, TkRange}
import org.ditw.tknr.Tokenizers.{TTokenizer, TokenSplitterCond, TokenizerSettings}

object TknrHelpers extends Serializable {
  private [ditw] val EmptyStr = ""
  private [ditw] def noPfxSfx(content:String) = IndexedSeq(content)
  private [ditw] def noPfx(content:String, sfx:String) = IndexedSeq(content, EmptyStr, sfx)
  private [ditw] def commaSfx(content:String) = noPfx(content, ",")
  private [ditw] def noSfx(content:String, pfx:String) = IndexedSeq(content, pfx)


  private [ditw] def tokenFrom(
    idx:Int,
    tokenContent:IndexedSeq[String]
  ):Token = {
    val content = tokenContent(0)
    val pfx = if (tokenContent.length > 1) tokenContent(1) else EmptyStr
    val sfx = if (tokenContent.length > 2) tokenContent(2) else EmptyStr
    new Token(
      idx=idx,
      content=content,
      pfx=pfx,
      sfx=sfx
    )
  }

  private [ditw] def tokensFrom(
    contents:IndexedSeq[IndexedSeq[String]]
  ):IndexedSeq[Token] = {
    contents.indices.map(idx => tokenFrom(idx, contents(idx)))
  }

  private [ditw] def origTokenStrsFrom(
    contents:IndexedSeq[IndexedSeq[String]]
  ):(String, IndexedSeq[String]) = {
    val origTokenStrs = contents.indices.map { idx =>
      val lineContents = contents(idx)
      val content = lineContents(0)
      val pfx = if (lineContents.length > 1) lineContents(1) else EmptyStr
      val sfx = if (lineContents.length > 2) lineContents(2) else EmptyStr
      s"$pfx$content$sfx"
    }
    val orig = origTokenStrs.mkString(" ")
    orig -> origTokenStrs
  }

  private [ditw] def loTFrom(
    contents:IndexedSeq[IndexedSeq[String]]
  ): SeqOfTokens = {
    val tokens = tokensFrom(contents)
    val (orig, origTokenStrs) = origTokenStrsFrom(contents)
    new SeqOfTokens(orig, origTokenStrs, tokens)
  }

  private [ditw] def resultFrom(
    orig:String,
    dict:Dict,
    sots:IndexedSeq[SeqOfTokens]
  ): TknrResult = {
    new TknrResult(orig, dict, sots)
  }

  private def tokenEqual(t1:Token, t2:Token):Boolean = {
    t1.content == t2.content &&
      t1.pfx == t2.pfx && t1.sfx == t2.sfx &&
      t1.idx == t2.idx
  }

  private def sotEqualTest(sot1:SeqOfTokens, sot2:SeqOfTokens):Boolean = {
    // sot1.orig == sot2.orig &&
      sot1.origTokenStrs == sot2.origTokenStrs &&
      sot1.tokens.size == sot2.tokens.size &&
      sot1.tokens.indices.forall(idx => tokenEqual(sot1.tokens(idx), sot2.tokens(idx)))
  }

  private [ditw] def resEqual(tr1:TknrResult, tr2:TknrResult):Boolean = {
    tr1.linesOfTokens.size == tr2.linesOfTokens.size &&
      tr1.linesOfTokens.indices.forall(idx => sotEqualTest(tr1.linesOfTokens(idx), tr2.linesOfTokens(idx)))
  }

  private [ditw] def rangeFromTp3(input: TknrResult, tp3:(Int, Int, Int)): TkRange =
    TkRange(input, tp3._1, tp3._2, tp3._3)

  private [ditw] val TokenSplitter_CommaColon = TokenSplitterCond(
    ".*[,;]([^\"]+|\".+)",
    "(?<=[,;])",
    Set(",;")
  )

  private val _dashSlashSepWords = Vector(
    "university",
    "univ",
    "colorado",
    "Pharmacy",
    "Hospital",
    "center",
    "centre",
    "College",
    "California",
    "Nebraska",
    "Missouri",
    "Wisconsin",
    "Illinois",
    "Medicine",
    "Miami",
    "Michigan",
    "Jersey",
    "School",
    "Carolina",
    "Clinic",
    "Winthrop",
    "Minnesota",
    "Institute",
    "Sanford",
    "Dana",
    "Sloan"
  ).mkString("|")

  val DashSlashChars:String = "-–/"
  val DashSlashSet:Set[String] = DashSlashChars.map(_.toString).toSet
  private [ditw] val TokenSplitter_DashSlash = TokenSplitterCond(
    s"(?i)(${_dashSlashSepWords})[\\-–/].*",
    "[\\-–/]+",
    DashSlashSet
  )

  val PunctChars = ".,;:()[]\""
  val _AffIndexChars =
    """╫╪^?ζΘΦΨχΣΠξΔǁ«»¿×®°±¹²³ª©*†‡§¶∥‖║#△■●□⊥∇⁎€⁴№∞∧∫∮≠⊕⊗⊗⊗⊞⋈⌋⑊┘┼┼□▰▲▼▽◆◇◊○◐★☆⚲⦀⧓⿿☼§𝕃�|£¤¥⟁◑◪⧖⧧⧨⧩⧫⬢⬡⬠⬟⧳̂⊗"""

  private val EmptySpecialTokens = Set[String]()
  private[tknr] val DefTokenSplitters2Keep = Set("/")
  def TknrTextSeg(
    specialTokens:Set[String] = EmptySpecialTokens
  ):TTokenizer = Tokenizers.load(
    TokenizerSettings(
      "\\n+",
      specialTokens,
      "[\\h]+",
      DefTokenSplitters2Keep,
      List(
        TokenSplitter_CommaColon, TokenSplitter_DashSlash
      ),
      Trimmers.byChars(
        _AffIndexChars + PunctChars
      )
    )
  )
}
