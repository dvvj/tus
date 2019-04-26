package org.ditw.textSeg.common
import org.ditw.common.Dict
import org.ditw.common.InputHelpers.splitVocabEntries
import org.ditw.matcher.TokenMatchers.ngramT
import org.ditw.matcher.{MatcherMgr, TCompMatcher, TPostProc, TTkMatcher}
import org.ditw.textSeg.SegMatchers._
import org.ditw.textSeg.common.Tags.TagGroup
import org.ditw.textSeg.common.Vocabs._

import scala.collection.mutable.ListBuffer

object CatSegMatchers {

  object Category extends Enumeration {
    type Category = Value
    val Univ, Corp, Hosp, ResInst = Value
  }

  import Category._

  private [textSeg] trait TSegMatchers4Cat extends Serializable {
    def cat:Category
    def tms:List[TTkMatcher]
    def cms:List[TCompMatcher]
    val tagGroup:TagGroup
    def postproc:TPostProc
  }

  private def createTmIfNonEmptyVoc(
    voc:Set[String],
    tag:String,
    addTo:ListBuffer[TTkMatcher],
    dict: Dict
  ):Unit = {
    if (voc.nonEmpty) {
      addTo += ngramT(
        splitVocabEntries(voc),
        dict,
        tag
      )
    }
  }

  private val EmptyTags = Set[String]()
  private [textSeg] class SegMatcher4Cat(
    val cat:Category,
    val tagGroup:TagGroup,
    keywords:Set[String],
    gazWords:Set[String],
    stopKeywords:Set[String],
    segStopTagsLeft:Set[String],
    segStopTagsRight:Set[String],
    canBeStart:Boolean,
    dict: Dict,
    extraTms:List[TTkMatcher] = Nil,
    extraCms:List[TCompMatcher] = Nil,
    extraPostProcs:List[TPostProc] = Nil
  ) extends TSegMatchers4Cat {

    val tms:List[TTkMatcher] = {
      val t = ListBuffer[TTkMatcher](
        ngramT(
          splitVocabEntries(keywords),
          dict,
          tagGroup.keywordTag
        )
      )
      createTmIfNonEmptyVoc(gazWords, tagGroup.gazTag, t, dict)
      createTmIfNonEmptyVoc(stopKeywords, tagGroup.stopWordsTag, t, dict)

      extraTms ++ t
    }

    val cms:List[TCompMatcher] = {
      val hasStopTags = segStopTagsLeft.nonEmpty || segStopTagsRight.nonEmpty

      val t =
        if (!hasStopTags) {
          segByPfxSfx(
            Set(tagGroup.keywordTag), _SegPfxs, _SegSfxs,
            canBeStart,
            tagGroup.segTag
          )
        }
        else {
          val t0 = segByPfxSfx(
            Set(tagGroup.keywordTag), _SegPfxs, _SegSfxs,
            canBeStart
          )
          segByTags(
            t0, segStopTagsLeft, segStopTagsRight, tagGroup.segTag
          )
        }

//      val leftStopTags =
//        if (segStopTagsLeft.nonEmpty) Set(tagGroup.segLeftStopTag)
//        else EmptyTags
//      val rightStopTags =
//        if (segStopTagsRight.nonEmpty) Set(tagGroup.segRightStopTag)
//        else EmptyTags

      t :: extraCms
    }


    private val orderedPostprocs:List[TPostProc] = {
      MatcherMgr.postProcBlocker(
        Map(tagGroup.stopWordsTag -> Set(tagGroup.segTag))
      ) :: extraPostProcs :::
      MatcherMgr.postProcOverride(
        Map(tagGroup.gazTag -> tagGroup.segTag)
      ) :: Nil
    }

    val postproc:TPostProc = MatcherMgr.postProcPrioList(orderedPostprocs)
  }
}
