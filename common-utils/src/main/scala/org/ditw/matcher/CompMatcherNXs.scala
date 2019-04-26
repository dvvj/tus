package org.ditw.matcher
import org.ditw.common.TkRange
import org.ditw.matcher.CompMatcherNs.TCompMatcherSeq

object CompMatcherNXs extends Serializable {
  import CompMatchers._

  private val EmptyMatches = Set[TkMatch]()

  private[matcher] class CmLookAround (
    private val sfx:Set[String],
    private val sfxCounts:(Int, Int),
    override protected val subMatchers:Iterable[TCompMatcher],
    protected val oneMatchEach:Boolean,
    protected val left2Right:Boolean,
    val tag:Option[String]
  ) extends TCompMatcher with TCompMatcherN with TDefRunAtLineFrom {
//    protected val subMatchers:Iterable[TCompMatcher] = Iterable(mCenter, m2Lookfor)
    private val mCenter:TCompMatcher = subMatchers.head
    private val m2Lookfor:TCompMatcher = subMatchers.tail.head

    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int): Set[TkMatch] = {
      val ctMatches = mCenter.runAtLine(matchPool, lineIdx)
      val lookforMatches = m2Lookfor.runAtLine(matchPool, lineIdx)

      if (ctMatches.nonEmpty) {
        if (!oneMatchEach) {
          ctMatches.flatMap { m =>
            val rangeBySfx = matchPool.input.linesOfTokens(lineIdx).rangeBySfxs(m.range, sfx, sfxCounts)
            lookforMatches.filter { lm => rangeBySfx.covers(lm.range) && !lm.range.overlap(m.range) }
              .map { lm =>
                val mseq = if (m.range.start < lm.range.start) IndexedSeq(m, lm)
                else IndexedSeq(lm, m)
                TkMatch.fromChildren(mseq)
              }
          }
        }
        else {
          // find first depending on left->right or right-> left
          ctMatches.flatMap { m =>
            val rangeBySfx = matchPool.input.linesOfTokens(lineIdx).rangeBySfxs(m.range, sfx, sfxCounts)
            val t = lookforMatches.filter { lm => rangeBySfx.covers(lm.range) && !lm.range.overlap(m.range) }
              .toList
            if (t.nonEmpty) {
              val sorted =
                if (left2Right) t.sortBy(_.range)
                else t.sortBy(_.range)(reverseOrdering)
              val lm = sorted.head
              val mseq = if (m.range.start < lm.range.start) IndexedSeq(m, lm)
              else IndexedSeq(lm, m)
              Option(TkMatch.fromChildren(mseq))
            }
            else EmptyMatches
          }
        }
      }
      else EmptyMatches

    }
  }

  private val reverseOrdering = new Ordering[TkRange] {
    override def compare(r1:TkRange, r2: TkRange): Int = {
      assert(r1.lineIdx == r2.lineIdx)
      val e = r2.end-r1.end  // big end goes first
      if (e == 0) {
        r1.start-r2.start // if end same, pick smaller [1, 3]  [2, 3]
      }
      else e
    }
  }

  def sfxLookAround_R2L(
    sfx:Set[String],
    sfxCounts:(Int, Int),
    mCenter:TCompMatcher,
    m2Lookfor:TCompMatcher,
    tag:String
  ):TCompMatcher = {
    new CmLookAround(sfx, sfxCounts, List(mCenter, m2Lookfor),
      true,
      false,
      Option(tag))
  }

  def sfxLookAround_ALL(
                         sfx:Set[String],
                         sfxCounts:(Int, Int),
                         mCenter:TCompMatcher,
                         m2Lookfor:TCompMatcher,
                         tag:String
                       ):TCompMatcher = {
    new CmLookAround(sfx, sfxCounts, List(mCenter, m2Lookfor),
      false,
      false, // N/A
      Option(tag))
  }


  def sfxLookAroundByTag_R2L(
    sfx:Set[String],
    sfxCounts:(Int, Int),
    mCenterTag:String,
    m2LookforTag:String,
    tag:String
  ):TCompMatcher = {
    sfxLookAround_R2L(sfx,
      sfxCounts,
      CompMatchers.byTag(mCenterTag),
      CompMatchers.byTag(m2LookforTag),
      tag
    )
  }

  def sfxLookAroundByTag_ALL(
                              sfx:Set[String],
                              sfxCounts:(Int, Int),
                              mCenterTag:String,
                              m2LookforTag:String,
                              tag:String
                            ):TCompMatcher = {
    sfxLookAround_ALL(sfx,
      sfxCounts,
      CompMatchers.byTag(mCenterTag),
      CompMatchers.byTag(m2LookforTag),
      tag
    )
  }
}
