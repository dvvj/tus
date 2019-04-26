package org.ditw.textSeg.catSegMatchers
import org.ditw.common.Dict
import org.ditw.matcher.{TCompMatcher, TTkMatcher}
import org.ditw.textSeg.common.CatSegMatchers.Category.Category
import org.ditw.textSeg.common.CatSegMatchers.{Category, SegMatcher4Cat, TSegMatchers4Cat}

object Cat1SegMatchers {

  import org.ditw.matcher.TokenMatchers._
  import org.ditw.common.InputHelpers._
  import org.ditw.textSeg.SegMatchers._
  import org.ditw.textSeg.common.Vocabs._
  import org.ditw.textSeg.common.Tags._
//  private val tmCorp = ngramT(
//    splitVocabEntries(_CorpWords),
//    _Dict,
//    TagTmCorp
//  )
//  private val segCorp = segByPfxSfx(
//    Set(TagTmCorp), _SegSfxs,
//    false,
//    TagSegCorp
//  )
//
//  private[textSeg] val segMatchers = new TSegMatchers4Cat {
//    override def cat: Category = Category.Corp
//    override def tms: List[TTkMatcher] = List(tmCorp)
//    override def cms: List[TCompMatcher] = List(segCorp)
//  }

  private[textSeg] def segMatchers(dict: Dict) = new SegMatcher4Cat(
    cat = Category.Corp,
    tagGroup = TagGroup4Corp,
    keywords = _CorpWords,
    gazWords = Set(),
    stopKeywords = Set(),
    segStopTagsLeft = Set(),
    segStopTagsRight = Set(),
    false,
    dict
  )

}
