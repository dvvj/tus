package org.ditw.textSeg
import org.ditw.common.TkRange
import org.ditw.matcher.CompMatchers.TDefRunAtLineFrom
import org.ditw.matcher.{MatchPool, TCompMatcher, TkMatch}
import org.ditw.textSeg.common.AssiMatchers

object SegMatchers {

  import org.ditw.matcher.CompMatcherNs._
  private val RangeBy2_1:(Int, Int) = (2, 1)

  private[textSeg] class SegBySfx(
    private val tagsContained:Set[String],
    private val sfxs:Set[String],
    private val canBeStart:Boolean = true,
    val tag:Option[String]
  ) extends TCompMatcher with TDefRunAtLineFrom {
    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int): Set[TkMatch] = {
      val matches = matchPool.get(tagsContained)
        .filter(_.range.lineIdx == lineIdx)
      val segMatches = matches.map { m =>
        val lot = matchPool.input.linesOfTokens(m.range.lineIdx)
        var newRange = lot.rangeBy(m.range, sfxs)
        if (!canBeStart && m.range.start == newRange.start) {
          newRange = lot.rangeBySfxs(m.range, sfxs, RangeBy2_1)
        }
        TkMatch.oneChild(newRange, m, tag)
      }
      segMatches
    }

    override def getRefTags(): Set[String] = tagsContained
  }

  private[textSeg] class SegEndWithTags(
    private val matcher:TCompMatcher,
    private val tagsToEndWith:Set[String],
    val tag:Option[String]
  ) extends TCompMatcher with TDefRunAtLineFrom {
    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int
    ): Set[TkMatch] = {
      val matches = matcher.runAtLine(matchPool, lineIdx)
      val endTagRanges = matchPool.get(tagsToEndWith)
        .map(_.range)
        .filter(_.lineIdx == lineIdx)
      val segMatches = matches.flatMap { m =>
        val allCovered = endTagRanges.filter(m.range.covers)
          .toList.sortBy(_.end)(Ordering[Int].reverse)
        if (allCovered.nonEmpty) {
          val maxEnd = allCovered.head.end
          val res =
            if (maxEnd == m.range.end) m
            else {
              val newRange = m.range.copy(end = maxEnd)
              TkMatch.updateRange(m, newRange)
            }
          Option(res)
        }
        else None
      }

      TkMatch.mergeByRange(segMatches)
    }

    override def getRefTags(): Set[String] = {
      tagsToEndWith ++ refTagsFromMatcher(matcher)
    }
  }

  private[textSeg] class SegByPfxSfx(
    private val tagsContained:Set[String],
    private val pfxs:Set[String],
    private val sfxs:Set[String],
    private val canBeStart:Boolean = true,
    val tag:Option[String]
  ) extends TCompMatcher with TDefRunAtLineFrom {
    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int
    ): Set[TkMatch] = {

      val matches = matchPool.get(tagsContained)
        .filter(_.range.lineIdx == lineIdx)
      val segMatches = matches.flatMap { m =>
        val lot = matchPool.input.linesOfTokens(m.range.lineIdx)
        var newRangeBySfx = lot.rangeBySfxs(m.range, sfxs)
        var newRangeByPfx = lot.rangeByPfxs(m.range, pfxs)
        var newRange = newRangeBySfx.intersect(newRangeByPfx)
        if (newRange.isEmpty) {
          println("error?")
        }
        if (!canBeStart && m.range.start == newRange.get.start) {
          if (newRangeByPfx.start == m.range.start)
            newRangeByPfx = lot.rangeByPfxs(m.range, pfxs, RangeBy2_1)
          if (newRangeBySfx.start == m.range.start)
            newRangeBySfx = lot.rangeBySfxs(m.range, sfxs, RangeBy2_1)
          newRange = newRangeBySfx.intersect(newRangeByPfx)
        }
        if (!canBeStart && m.range.start == newRange.get.start) {
          // still failed (probably at the beginning) give up
          None
        }
        else Option(TkMatch.oneChild(newRange.get, m, tag))
      }

      segMatches
    }

    override def getRefTags(): Set[String] = tagsContained
  }


  private[textSeg] class SegByTags(
    private val matcher:TCompMatcher,
    private val leftTags:Set[String],
    private val rightTags:Set[String],
    val tag:Option[String]
  ) extends TCompMatcher with TDefRunAtLineFrom {
    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int
    ): Set[TkMatch] = {
      val candidates = matcher.runAtLine(matchPool, lineIdx)
      val leftRanges = matchPool.get(leftTags).filter(_.range.lineIdx == lineIdx)
        .map(_.range)
      val rightRanges = matchPool.get(rightTags).filter(_.range.lineIdx == lineIdx)
        .map(_.range)
      val c1 = candidates.map { c =>
        var maxLeft = c.range.start
        val firstChildStart = c.children.head.range.start // todo: what if no child?
        leftRanges.foreach { lr =>
          if (lr.overlap(c.range)) {
            if (lr.end > maxLeft && lr.end <= firstChildStart) {
              maxLeft = lr.end
            }
          }
        }
        val newStart = maxLeft

        val lastChildEnd = c.children.last.range.end
        var minRight = c.range.end
        rightRanges.foreach { rr =>
          if (rr.overlap(c.range)) {
            if (rr.start < minRight && rr.start >= lastChildEnd) {
              minRight = rr.start
            }
          }
        }
        val newEnd = minRight
        if (newStart != c.range.start || newEnd != c.range.end) {
          TkMatch.noChild(
            matchPool,
            TkRange(matchPool.input, lineIdx, newStart, newEnd),
            tag
          )
        }
        else c
      }
//      val t1 = matchPool.get(AssiMatchers._CmXDeptTag)
//      println(s"---- $t1\n\t$c1")
      c1
    }

    private val refTags = leftTags ++ rightTags ++ refTagsFromMatcher(matcher)
    override def getRefTags(): Set[String] = refTags
  }

  def segBySfx(
    tagsContained:Set[String],
    sfxs:Set[String],
    canBeStart:Boolean,
    tag:String
  ):TCompMatcher = {
    new SegBySfx(tagsContained, sfxs, canBeStart, Option(tag))
  }

  def segByPfxSfx(
                tagsContained:Set[String],
                pfxs:Set[String],
                sfxs:Set[String],
                canBeStart:Boolean,
                tag:String
              ):TCompMatcher = {
    new SegByPfxSfx(tagsContained, pfxs, sfxs, canBeStart, Option(tag))
  }
  def segByPfxSfx(
                   tagsContained:Set[String],
                   pfxs:Set[String],
                   sfxs:Set[String],
                   canBeStart:Boolean
                 ):TCompMatcher = {
    new SegByPfxSfx(tagsContained, pfxs, sfxs, canBeStart, None)
  }

  def segByTags(
    matcher:TCompMatcher,
    leftTags:Set[String],
    rightTags:Set[String],
    tag:String
  ):TCompMatcher = {
    new SegByTags(matcher, leftTags, rightTags, Option(tag))
  }

  def endWithTags(
    matcher:TCompMatcher,
    tagsToEndWith:Set[String],
    tag:String
  ):TCompMatcher = {
    new SegEndWithTags(matcher, tagsToEndWith, Option(tag))
  }
}
