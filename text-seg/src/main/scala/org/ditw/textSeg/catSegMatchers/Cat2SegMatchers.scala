package org.ditw.textSeg.catSegMatchers
import org.ditw.common.{Dict, InputHelpers, ResourceHelpers}
import org.ditw.matcher.{CompMatcherNs, MatcherMgr, TokenMatchers}
import org.ditw.matcher.TokenMatchers.ngramT
import org.ditw.textSeg.SegMatchers._
import org.ditw.textSeg.common.CatSegMatchers.{Category, SegMatcher4Cat, TSegMatchers4Cat}
import org.ditw.textSeg.common.Tags._
import org.ditw.textSeg.common.{AssiMatchers, Tags, Vocabs}
import org.ditw.textSeg.common.Vocabs._

object Cat2SegMatchers {
  import org.ditw.matcher.CompMatchers

//  private val tmUniv = ngramT(
//    splitVocabEntries(_UnivWords),
//    _Dict,
//    TagGroup4Univ.segTag
//  )
//
//  private val segUniv = segByPfxSfx(
//    Set(TagTmUniv), _SegSfxs,
//    true,
//    TagGroup4Univ
//  )
//
//  private[textSeg] val segMatchers = new TSegMatchers4Cat {
//    override def cat: Category = Category.Univ
//    override def tms: List[TTkMatcher] = List(tmUniv)
//    override def cms: List[TCompMatcher] = List(segUniv)
//  }
  private[textSeg] val tagTmUnivOf = customTmTag("UnivOfVocab")
  private[textSeg] def tmUnivOf(dict: Dict) = ngramT(
    InputHelpers.splitVocabEntries(Vocabs.__univOfVocab),
    dict,
    tagTmUnivOf
  )

  private val _canBeStart = true
  private[textSeg] val tagCmUnivOf = customCmTag("UnivOfVocab")
  private val cmUnivOf = CompMatcherNs.lngOfTags(
    IndexedSeq(TagGroup4Univ.keywordTag, TmOf, tagTmUnivOf),
    tagCmUnivOf
  )
  private[textSeg] val tagSegUnivOf = customCmTag("SegUnivOf")
  private[textSeg] val tagSegUnivOfVocabEW = customCmTag("SegUnivOfVocabEW")
  private[textSeg] val tagSegUnivOfVocab = customCmTag("SegUnivOfVocab")
  private[textSeg] val segUnivOf = segByPfxSfx(
    Set(tagCmUnivOf), _SegPfxs, _SegSfxs,
    _canBeStart,
    tagSegUnivOf
  )
  import CompMatchers._
  private[textSeg] val segUnivOfVocabEW =
    endWithTags(
      byTag(tagSegUnivOf),
      Set(tagTmUnivOf),
      tagSegUnivOfVocabEW
  )
  private[textSeg] val segUnivOfVocab = segByTags(
    byTag(tagSegUnivOfVocabEW),
    Set(AssiMatchers._CmDeptOfTag),
    Set(),
    tagSegUnivOfVocab
  )

  private[textSeg] val tagCmUnivOfOf = customCmTag("UnivOfVocabOf")
  private val cmUnivOfOf = CompMatcherNs.lngOfTags(
    IndexedSeq(tagCmUnivOf, TmOf),
    tagCmUnivOfOf
  )
  private[textSeg] val tagSegUnivOfVocabOf = customCmTag("SegUnivOfVocabOf")
  private val segUnivOfVocabOf = segByPfxSfx(
    Set(tagCmUnivOfOf), _SegPfxs, _SegSfxs,
    _canBeStart,
    tagSegUnivOfVocabOf
  )


//  private[textSeg] val tagSegUnivInBrackets = customCmTag("SegUnivInBrackets")
//  private[textSeg] val segUnivInBracket = segByPfxSfx(
//    Set(TagGroup4Univ.keywordTag), _SegPfxs_Bracket, _SegSfxs_Bracket,
//    _canBeStart,
//    tagSegUnivInBrackets
//  )
//  private[textSeg] val univInBracketsOverride = MatcherMgr.postProcOverride(
//    Map(
//      tagSegUnivInBrackets -> TagGroup4Univ.segTag
//    )
//  )
  private[textSeg] val univOfVocabOverride = MatcherMgr.postProcOverride(
    Map(
      tagSegUnivOfVocab -> TagGroup4Univ.segTag
    )
  )
  private[textSeg] val univOfVocabOfOverride = MatcherMgr.postProcOverride(
    Map(
      tagSegUnivOfVocabOf -> TagGroup4Univ.segTag
    )
  )

  private val UnivTagGroup = Tags.TagGroup4Univ

  private def tmSegStopLeft(dict:Dict) = TokenMatchers.ngramT(
    InputHelpers.splitVocabEntries(_UnivSegStopWordsLeft), dict, UnivTagGroup.segLeftStopTag)
  private def tmSegStopRight(dict:Dict) = TokenMatchers.ngramT(
    InputHelpers.splitVocabEntries(_UnivSegStopWordsRight), dict, UnivTagGroup.segRightStopTag)

  def segMatchers(dict:Dict) = new SegMatcher4Cat(
    cat = Category.Univ,
    tagGroup = TagGroup4Univ,
    keywords = _UnivWords,
    gazWords = _UnivGazWords,
    stopKeywords = _UnivStopWords,
    segStopTagsLeft = Set(UnivTagGroup.segLeftStopTag, AssiMatchers._CmDeptOfTag, TmSingleLowerAZ, TmEmail, TmDigits, TmDigitsDashDigits),
    segStopTagsRight = Set(UnivTagGroup.segRightStopTag, AssiMatchers._CmXDeptTag, TmEmail, TmDigits, TmDigitsDashDigits),
    _canBeStart,
    dict,
    List(tmUnivOf(dict), tmSegStopLeft(dict), tmSegStopRight(dict)),
    List(cmUnivOf, segUnivOf, segUnivOfVocabEW, segUnivOfVocab, cmUnivOfOf, segUnivOfVocabOf),
    List(univOfVocabOverride, univOfVocabOfOverride)
  )
}
