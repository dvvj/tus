package org.ditw.tknr
import org.ditw.common.{Dict, TkRange}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{IndexedSeqLike, mutable}

class SeqOfTokens(
  val orig:String,
  val origTokenStrs:IndexedSeq[String],
  _tokens: Seq[Token]
) extends IndexedSeq[Token] with IndexedSeqLike[Token, SeqOfTokens] with Serializable {

  import SeqOfTokens._

  private[ditw] val tokens:IndexedSeq[Token] = {
    _tokens.foreach(_.setLoT(this))
    _tokens.toIndexedSeq
  }
  override def apply(idx:Int):Token = tokens(idx)

  override def length: Int = tokens.length

  override def newBuilder: mutable.Builder[Token, SeqOfTokens] =
    _newBuilder

  override def toString(): String = {
    s"[$orig] size=$length"
  }

  def trOrigTokens(start:Int, end:Int):String = {
    origTokenStrs.slice(start, end).mkString(" ")
  }

  def rangeBySfxs(
    range: TkRange,
    sfxs:Set[String],
    sfxCounts:(Int, Int) = RangeBySfxCountsDefault
  ):TkRange = {
    rangeBy(range, sfxs, false, sfxCounts)
  }

  def rangeByPfxs(
    range: TkRange,
    pfxs:Set[String],
    sfxCounts:(Int, Int) = RangeBySfxCountsDefault
  ):TkRange = {
    rangeBy(range, pfxs, true, sfxCounts)
  }

  def rangeBy(
    range: TkRange,
    psfxs:Set[String],
    isPrefix:Boolean = false,
    sfxCounts:(Int, Int) = RangeBySfxCountsDefault
  ):TkRange = {
    val (leftCount, rightCount) = sfxCounts

    var found = false
    var start = -1
    if (leftCount == 0)
      start = range.start
    else {
      start = if (isPrefix) range.start else range.start-1
      var leftFound = 0
      while (start >= 0 && !found) {
        val token = tokens(start)
        val psfx = if (isPrefix) token.pfx else token.sfx
        if (checkPSfx(psfx, psfxs) ||
          (token.content.isEmpty && psfxs.contains(token.str))
        ) {
          leftFound += 1
          if (leftFound >= leftCount)
            found = true
        }
        if (!found)
          start -= 1
      }
      start =
        if (found) {
          if (!isPrefix) start+1
          else start
        }
        else 0
    }

    var end = -1
    if (rightCount == 0)
      end = range.end
    else {
      end = if (isPrefix) range.end else range.end-1
      found = false
      var rightFound = 0
      while (end < tokens.size && !found) {
        val token = tokens(end)
        val psfx = if (isPrefix) token.pfx else token.sfx
        if (checkPSfx(psfx, psfxs) ||
          (token.content.isEmpty && psfxs.contains(token.str))
        ) {
          rightFound += 1
          if (rightFound >= rightCount)
            found = true
        }
        if (!found)
          end += 1
      }
      end = if (found) {
        // if the (end) token is ',' self, use end instead
        if (tokens(end).content.isEmpty) end
        else {
          if (!isPrefix) end+1
          else end
        }
      }
      else tokens.size
    }
    TkRange(range.input, range.lineIdx, start, end)
  }
}

object SeqOfTokens {

  private def checkPSfx(psfx:String, sfxSet:Set[String]):Boolean = {
    sfxSet.exists(psfx.contains)
  }

  def fromTokens(tokens:Seq[Token]):SeqOfTokens = {
    val origTokenStrs = tokens.map { t =>
      s"${t.pfx}${t.content}${t.sfx}"
    }
    val orig = origTokenStrs.mkString(" ")
    val reIndexed = tokens.indices.map(idx => tokens(idx).reIndex(idx))
    new SeqOfTokens(orig, origTokenStrs.toIndexedSeq, reIndexed)
  }
  private def _newBuilder: mutable.Builder[Token, SeqOfTokens] =
    new ArrayBuffer[Token] mapResult fromTokens

  private[tknr] val RangeBySfxCountsDefault = (1, 1)
}

