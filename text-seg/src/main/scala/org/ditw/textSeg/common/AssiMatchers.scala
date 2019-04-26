package org.ditw.textSeg.common
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.common.InputHelpers.splitVocabEntries
import org.ditw.matcher.TokenMatchers.ngramT
import org.ditw.matcher.{CompMatcherNs, TCompMatcher, TokenMatchers}

object AssiMatchers extends Serializable {
  import Vocabs._
  import Tags._

  private val _TmDept = TmTagPfx + "Dept"
  private val _TmDeptType = TmTagPfx + "DeptType"


  private val _ExtraTmData = List(
    TmOf -> Set("of"),
    _TmDept -> _DeptWords,
    _TmDeptType -> _DeptTypes,
    TmAnd -> _And
  )

  import TokenMatchers._
  private[textSeg] def _ExtraTms(dict:Dict) = _ExtraTmData.map(
    tmd => ngramT(splitVocabEntries(tmd._2), dict, tmd._1)
  ) ++ List(
    singleLowerAZMatcher(TmSingleLowerAZ),
    emailMatcher(TmEmail),
    digitsMatcher(TmDigits),
    digitsDashDigitsMatcher(TmDigitsDashDigits)
  )

  private val _CmDeptTypeSeqTag = CmTagPfx + "DeptTypeSeq"
  private[textSeg] val _CmDeptTypes = CompMatcherNs.entSeq(
    Set(_TmDeptType), Set(TmAnd), true, _CmDeptTypeSeqTag
  )

  private[textSeg] val _CmDeptOfTag = CmTagPfx + "DeptOf"
  private val _CmDeptOf = CompMatcherNs.lngOfTags(
    IndexedSeq(_TmDept, TmOf, _CmDeptTypeSeqTag),
    _CmDeptOfTag
  )
  private[textSeg] val _CmXDeptTag = CmTagPfx + "XDept"
  private val _CmXDept = CompMatcherNs.lngOfTags(
    IndexedSeq(_CmDeptTypeSeqTag, _TmDept),
    _CmXDeptTag
  )
  private[textSeg] val _ExtraCms:List[TCompMatcher] = List(_CmDeptTypes, _CmDeptOf, _CmXDept)


}
