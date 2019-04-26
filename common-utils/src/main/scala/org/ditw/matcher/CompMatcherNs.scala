package org.ditw.matcher
import scala.collection.mutable.ListBuffer

object CompMatcherNs {
  import CompMatchers._

  def refTagsFromMatcher(matcher:TCompMatcher):Iterable[String] =
    if (matcher.tag.nonEmpty) matcher.tag
    else matcher.getRefTags()
  // ------------ OR
  private[matcher] class CmOr(
    protected val subMatchers:Set[TCompMatcher],
    val tag:Option[String]
  ) extends TCompMatcher with TCompMatcherN {
    override def runAtLineFrom(
      matchPool: MatchPool,
      lineIdx: Int,
      start: Int): Set[TkMatch] = {
      subMatchers.flatMap(_.runAtLineFrom(matchPool, lineIdx, start))
    }
  }

  def or(subMatchers:Set[TCompMatcher], tag:String):TCompMatcher = {
    new CmOr(subMatchers, Option(tag))
  }

  private val EmptyMatches = List[IndexedSeq[TkMatch]]()
  private def _seqMatches(
    curr:List[IndexedSeq[TkMatch]],
    subMatchesSeq:IndexedSeq[Set[TkMatch]],
    start:Int
  ):List[IndexedSeq[TkMatch]] = {
    if (start == subMatchesSeq.size)
      curr
    else {
      val nextSubMatches = subMatchesSeq(start)
      val next = ListBuffer[IndexedSeq[TkMatch]]()
      if (nextSubMatches.nonEmpty) {
        if (curr.isEmpty) {
          next ++= nextSubMatches.map(sm => IndexedSeq(sm))
        }
        else {
          nextSubMatches.foreach { nextSubMatch =>
            curr.foreach { subMatchesSoFar =>
              val lastMatch = subMatchesSoFar.last
              if (nextSubMatch.range.start >= lastMatch.range.end) {
                next += (subMatchesSoFar :+ nextSubMatch)
              }
            }
          }
        }
        if (next.nonEmpty)
          _seqMatches(next.toList, subMatchesSeq, start+1)
        else
          EmptyMatches
      }
      else
        EmptyMatches
    }
  }

  private def seqMatches(
    subMatchesSeq:IndexedSeq[Set[TkMatch]]
  ):List[IndexedSeq[TkMatch]] = {
    _seqMatches(List(), subMatchesSeq, 0)
  }

  // ------------ Seq
  trait TCompMatcherSeq extends TCompMatcherN with TDefRunAtLineFrom {
    protected val subMatchers:IndexedSeq[TCompMatcher]

    private def matchCandidates(
      matchPool: MatchPool,
      subMatchesSeq:IndexedSeq[Set[TkMatch]]
    ):IndexedSeq[IndexedSeq[TkMatch]] = {
      val lineIdx2Matches = matchPool.input.linesOfTokens.indices.map { idx =>
        val matchesSeqInLineIdx = subMatchesSeq.map(_.filter(_.range.lineIdx == idx))
        idx -> matchesSeqInLineIdx
      }
      val matchesInLines = lineIdx2Matches.map(p => seqMatches(p._2))
//      val res = matchesInLines.flatMap(l => l.map(ch => TkMatch.fromChildren(ch)))
//      res.toSet
      matchesInLines.flatten
    }

    protected def filterCandidates(candidates:IndexedSeq[IndexedSeq[TkMatch]]):Set[TkMatch]

    override def run(matchPool: MatchPool)
      : Set[TkMatch] = {
      val subMatchesSeq:IndexedSeq[Set[TkMatch]] = subMatchers.map(_.run(matchPool))
      val candidates = matchCandidates(matchPool, subMatchesSeq)
      filterCandidates(candidates)
    }

//    private val _refTags:Set[String] = {
//      subMatchers.flatMap(sm => if (sm.tag.nonEmpty) sm.tag else sm.getRefTags())
//        .toSet
//    }
//
//    override def getRefTags: Set[String] = _refTags
  }

  private[matcher] class CmLNGram(
    protected val subMatchers:IndexedSeq[TCompMatcher],
    val tag:Option[String]
  ) extends TCompMatcherSeq {
    override protected def filterCandidates(candidates: IndexedSeq[IndexedSeq[TkMatch]])
      : Set[TkMatch] = {
      val filtered = candidates.filter { ml =>
        val nextToEachOther = (0 to ml.size-2).forall { idx =>
          val curr = ml(idx)
          val next = ml(idx+1)
          curr.range.end == next.range.start
        }
        nextToEachOther
      }

      filtered.map { subMatches =>
        val m = TkMatch.fromChildren(subMatches)
        if (tag.nonEmpty)
          m.addTag(tag.get)
        m
      }.toSet
    }
  }

  def lng(
    subMatchers:IndexedSeq[TCompMatcher],
    tag:String
  ):TCompMatcher = {
    new CmLNGram(subMatchers, Option(tag))
  }

  def lngOfTags(
    subMatcherTags:IndexedSeq[String],
    tag:String
  ):TCompMatcher = {
    lng(subMatcherTags.map(byTag), tag)
  }

  private[matcher] class CmEntSeq(
    private val entTags:Set[String],
    private val connTags:Set[String],
    private val commaInBtw:Boolean,
    val tag:Option[String]
  ) extends TCompMatcher with TDefRunAtLineFrom {
    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int): Set[TkMatch] = {
      val entMatches = matchPool
        .get(entTags)
        .filter(_.range.lineIdx == lineIdx)
        .toIndexedSeq
      val connMatches = matchPool
        .get(connTags)
        .filter(_.range.lineIdx == lineIdx)
        .map(m => m.range.start -> m)
        .groupBy(_._1)
        .mapValues(_.map(_._2))

      val res = ListBuffer[TkMatch]()
      val sorted = entMatches.sortBy(_.range)
      var currSoFar = ListBuffer[TkMatch]()
      sorted.indices.foreach { idx =>
        val curr = sorted(idx)
        if (currSoFar.isEmpty) {
          currSoFar += curr
        }
        else {
          val last = currSoFar.last
          if (curr.range.end > last.range.end) {
            if (connMatches.contains(last.range.end)) {
              val cands:Iterable[TkMatch] = connMatches(last.range.end)
              val filtered = cands.filter(_.range.end == curr.range.start)
              if (filtered.nonEmpty) {
                currSoFar ++= List(filtered.head, curr)
              }
              else {
                res += TkMatch.fromChildren(currSoFar.toIndexedSeq)
                currSoFar = ListBuffer[TkMatch]()
              }
            }
            else {
              if (commaInBtw) {
                if (last.range.end == curr.range.start &&
                  last.range.suffixedBy(","))
                  currSoFar += curr
                else {
//                  if (last.range.end < curr.range.start) {
                    res += TkMatch.fromChildren(currSoFar.toIndexedSeq)
                    currSoFar = ListBuffer[TkMatch]()
//                  }
//                  else {
//                    // ignore shorter terms (e.g. Biology vs Developmental Biology
//                  }
                }
              }
              else {
                res += TkMatch.fromChildren(currSoFar.toIndexedSeq)
                currSoFar = ListBuffer[TkMatch]()
              }
            }
          }
          else {
            // ignore shorter terms (e.g. Biology vs Developmental Biology
          }
        }
      }
      if (currSoFar.nonEmpty)
        res += TkMatch.fromChildren(currSoFar.toIndexedSeq)

      res.toSet
    }

    private val _refTags = entTags ++ connTags
    override def getRefTags(): Set[String] = _refTags
  }

  def entSeq(
    entTags:Set[String],
    connTags:Set[String],
    commaInBtw:Boolean,
    tag:String
  ):TCompMatcher = {
    new CmEntSeq(entTags, connTags, commaInBtw, Option(tag))
  }

}
