package org.ditw.textSeg.common

object Tags extends Serializable {

  private def builtInTag(tagStem:String) = {
    "__TagBITm" + tagStem
  }
  private[textSeg] val TmOf = builtInTag("Of")
  private[textSeg] val TmAnd = builtInTag("And")
  private[textSeg] val TmEmail = builtInTag("Email")
  private[textSeg] val TmDigits = builtInTag("Digits")
  private[textSeg] val TmDigitsDashDigits = builtInTag("D-D")
  private[textSeg] val TmSingleLowerAZ = builtInTag("SingleLowerAZ")

  private[textSeg] val TmTagPfx = "__TagTm"
  private[textSeg] val CmTagPfx = "__TagCm"
  private val GazTagPfx = "__TagGaz"
  private val TmStopTagPfx = "__TagStopTm"
  private val SegTagPfx = "_TagSeg"
  private val SegLeftStopTagPfx = "_TagSegLStop"
  private val SegRightStopTagPfx = "_TagSegRStop"

  private val CustomTmTagPrefix = "__CusTm"
  def customTmTag(tagStem:String):String = CustomTmTagPrefix + tagStem
  private[textSeg] val CustomCmTagPrefix = "_CusCm"
  def customCmTag(tagStem:String):String = CustomCmTagPrefix + tagStem

  case class TagGroup(
    keywordTag:String,
    gazTag:String,
    segTag:String,
    stopWordsTag:String,
    segLeftStopTag:String,
    segRightStopTag:String
  )

  private def tagGroup(groupTag:String):TagGroup = TagGroup(
    TmTagPfx + groupTag, // tm
    GazTagPfx + groupTag,
    SegTagPfx + groupTag, // seg
    TmStopTagPfx + groupTag, // stop words tag
    SegLeftStopTagPfx + groupTag, // seg stop left
    SegRightStopTagPfx + groupTag // seg stop right
  )

  private val Tag4Corp = "Corp"
  val TagGroup4Corp:TagGroup = tagGroup(Tag4Corp)

  private val Tag4Univ = "Univ"
  val TagGroup4Univ:TagGroup = tagGroup(Tag4Univ)
}
