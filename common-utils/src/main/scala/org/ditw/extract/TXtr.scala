package org.ditw.extract
import org.ditw.common.TkRange
import org.ditw.matcher.{MatchPool, TkMatch}

import scala.util.matching.Regex

trait TXtr[R] extends Serializable {

  def canApply(tag:String):Boolean
//  def extract(tag:String, m: TkMatch):List[R] = {
//    if (canApply(tag)) _extract(m)
//    else List()
//  }

  def extract(m: TkMatch):List[R]

  def extractAll(matchPool: MatchPool):Iterable[(TkRange, List[R])] = {
    val range2Exrs = matchPool.allTags().flatMap { tag =>
      if (!canApply(tag)) None
      else {
        val matches = matchPool.get(tag)
        matches.flatMap { m =>
          val exr = extract(m)
          if (exr.nonEmpty)
            Option(m.range -> exr)
          else None
        }
      }
    }
    range2Exrs
  }
}

object TXtr extends Serializable {

//  def runXtr[R : TXtr](m:TkMatch):List[R] =
//    implicitly[TXtr[R]].extract(m)
//  def runXtr[R : TXtr](m:Iterable[TkMatch]):Map[TkRange, List[R]] =
//    implicitly[TXtr[R]].extractAll(m)

  abstract class XtrExactTag[R](protected val tagToMatch:String) extends TXtr[R] {
    override def canApply(tag: String): Boolean = tag == tagToMatch
  }

  abstract class XtrPfx[R](protected val tagPfx:String) extends TXtr[R] {
    override def canApply(tag: String): Boolean = tag.startsWith(tagPfx)
  }

  abstract class XtrRegexTag[R](protected val regex:Regex) extends TXtr[R] {
    override def canApply(tag: String): Boolean = regex.pattern.matcher(tag).matches()
  }

  private def fullStrExactTagMatch(tag:String):TXtr[String] = new XtrExactTag[String](tag) {
    override def extract(m: TkMatch)
      : List[String] = List(m.range.str)
  }

  private def fullStrRegexTagMatch(regex:Regex):TXtr[String] = new XtrRegexTag[String](regex) {
    override def extract(m: TkMatch)
    : List[String] = List(m.range.str)
  }
}