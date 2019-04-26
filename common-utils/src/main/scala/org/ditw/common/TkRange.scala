package org.ditw.common
import org.ditw.tknr.{TknrResult, Token}

case class TkRange(
  input:TknrResult,
  lineIdx:Int,
  start:Int,
  end:Int
) extends Ordered[TkRange] {
  override def hashCode(): Int = {
    (lineIdx << 24) + (start << 16) + (end << 8) + input.hashCode()
  }

  override def equals(obj: Any): Boolean = obj match {
    case r:TkRange => {
      r.lineIdx == lineIdx && r.start == start && r.end == end
    }
    case _ => false
  }

  private val lastToken:Token = input.linesOfTokens(lineIdx).tokens(end-1)
  def suffixedBy(sfx:String):Boolean = {
    lastToken.sfx.contains(sfx)
  }

  def origStr:String = {
    val sot = input.linesOfTokens(lineIdx)
    sot.origTokenStrs.slice(start, end).mkString(" ")
  }

  def str:String = {
    val sot = input.linesOfTokens(lineIdx)
    sot.slice(start, end).map(_.content).mkString(" ")
  }

  def overlap(r2:TkRange):Boolean = {
    if (lineIdx == r2.lineIdx) {
      (start >= r2.start && start < r2.end) ||
        (r2.start >= start && r2.start < end)
    }
    else false
  }

  def intersect(r2:TkRange):Option[TkRange] = {
    if (overlap(r2)) {
      Option(
        TkRange(
          input, lineIdx,
          if (start >= r2.start) start else r2.start,
          if (end <= r2.end) end else r2.end
        )
      )
    }
    else None
  }

  def covers(r2:TkRange):Boolean = {
    if (lineIdx == r2.lineIdx) {
      start <= r2.start && end >= r2.end
    }
    else false
  }

  override def toString: String = {
    val startEnd =
      if (end - start > 1) s"$start,$end"
      else start.toString
    val lineStartEnd =
      if (input.linesOfTokens.size > 1)
        s"(L$lineIdx,$startEnd)"
      else
        s"($startEnd)"
    val trSrc = input.linesOfTokens(lineIdx).trOrigTokens(start, end)
    s"$trSrc$lineStartEnd"
  }

  import math.Ordered.orderingToOrdered
  override def compare(that: TkRange): Int = {
    (lineIdx, start, end) compare (that.lineIdx, that.start, that.end)
  }
}
