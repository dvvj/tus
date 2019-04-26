package org.ditw.demo1.matchers
import org.ditw.common.TkRange
import org.ditw.demo1.extracts.Xtrs
import org.ditw.demo1.gndata.{GNLevel, GNSvc}
import org.ditw.matcher.CompMatcherNs.lng
import org.ditw.matcher.CompMatchers.{TDefRunAtLineFrom, byTag}
import org.ditw.matcher.{MatchPool, TCompMatcher, TCompMatcherN, TkMatch}

import scala.collection.mutable.ListBuffer

object GNMatchers extends Serializable {

  import org.ditw.matcher.CompMatcherNs._
  def lngOfTagsSharedSfx(
    subMatcherTags:IndexedSeq[String],
    sharedSuffix:String,
    tag:String
  ):TCompMatcher = {
    val tagsWithSuffix = subMatcherTags.map(_ + sharedSuffix)
    lngOfTags(tagsWithSuffix, tag)
  }


  private def checkIfGNContains(gnsvc: GNSvc, gnid1:Long, gnid2:Long):Boolean = {
    val ent1 = gnsvc.entById(gnid1).get
    val ent2 = gnsvc.entById(gnid2).get

    GNSvc.checkContains(ent1, ent2)
  }

  private def checkIfContains(gnsvc: GNSvc, m1:TkMatch, m2:TkMatch):Boolean = {
    val ids1 = Xtrs.extractEntId(m1)
    val ids2 = Xtrs.extractEntId(m2)
    ids1.exists( id1 =>
      ids2.exists(id2 => checkIfGNContains(gnsvc, id1, id2))
    )
  }

  private def checkIsAdm1(gnsvc: GNSvc, m:TkMatch):Boolean = {
    val ids = Xtrs.extractEntId(m)
    ids.exists { id =>
      val e = gnsvc.entById(id)
      e.nonEmpty && e.get.isAdm && e.get.level == GNLevel.ADM1
    }
  }

  private val EmptyMatches = Set[TkMatch]()
  private[demo1] class GNSeqMatcher(
    protected val subMatchers: Iterable[TCompMatcher],
    private val sfx:Set[String],
    private val sfxCounts:(Int, Int),
    private val mustEndWithAdm1:Boolean,
    private val gnsvc: GNSvc,
    val tag:Option[String]
  ) extends TCompMatcher with TCompMatcherN with TDefRunAtLineFrom {
    val leafMatcher:TCompMatcher = subMatchers.head
    val nodeMatcher:TCompMatcher = subMatchers.tail.head

    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int): Set[TkMatch] = {
      val leafMatches = leafMatcher.runAtLine(matchPool, lineIdx)
      if (leafMatches.nonEmpty) {
        val nodeMatches = nodeMatcher.runAtLine(matchPool, lineIdx)
        if (nodeMatches.nonEmpty) {
          val srcLine = matchPool.input.linesOfTokens(lineIdx)
          val sortedNodes = nodeMatches.toList.sortBy(_.range)
          val res = ListBuffer[TkMatch]()
          leafMatches.foreach { lm =>
            val resNodes = ListBuffer[TkMatch]()
            resNodes += lm
            var cont = true
            var currRange = lm.range
            while (cont) {
              cont = false
              val rngBySfx = srcLine.rangeBySfxs(currRange, sfx, sfxCounts)
              val tgtRange = TkRange(matchPool.input, lineIdx, currRange.end, rngBySfx.end)
              val firstIn = sortedNodes.find { n =>
                tgtRange.covers(n.range) && checkIfContains(gnsvc, n, resNodes.last)
              }
              if (firstIn.nonEmpty) {
                val n = firstIn.get
                resNodes += n
                currRange = TkRange(matchPool.input, lineIdx, currRange.start, n.range.end)
                cont = true
              }
            }
            if (resNodes.size > 1) {
              val toAdd =
                if (mustEndWithAdm1)
                  checkIsAdm1(gnsvc, resNodes.last)
                else true
              if (toAdd)
                res += TkMatch.fromChildren(resNodes.toIndexedSeq)
            }
          }
          mergeMatches(res)
        }
        else EmptyMatches
      }
      else EmptyMatches
    }

    private def mergeMatches(matches:Iterable[TkMatch]):Set[TkMatch] = {
      val maxSz = matches.map(_.children.size).max
      val maxSzMatches = matches.filter(_.children.size == maxSz)
      if (maxSzMatches.size > 1) {
        maxSzMatches.toSet // todo
      }
      else maxSzMatches.toSet
    }
  }

  private val SfxComma = Set(",")
  private val DefSfxCounts = (1, 3)




  import org.ditw.matcher.CompMatchers._
  def GNSeqByTags(
    cityTag:String,
    adm1Tag:String,
    gnsvc: GNSvc,
    tag:String
  ):TCompMatcher = {
    new GNSeqMatcher(
      List(byTag(cityTag), byTag(adm1Tag)),
      SfxComma,
      DefSfxCounts,
      true,
      gnsvc,
      Option(tag)
    )
  }
}
