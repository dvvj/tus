package org.ditw.matcher
import scala.collection.mutable

trait TTkMatcher extends Serializable {
  val tag:Option[String]
  def addTagIfNonEmpty(matches:Iterable[TkMatch]):Unit = {
    if (tag.nonEmpty) {
      matches.foreach(_.addTag(tag.get))
    }
  }
  def run(matchPool: MatchPool)
  : Set[TkMatch] = {
    val res = mutable.Set[TkMatch]()

    matchPool.input.linesOfTokens.indices.foreach { lineIdx =>
      res ++= runAtLine(matchPool, lineIdx)
    }

    addTagIfNonEmpty(res)
    res.toSet
  }

  def runAtLine(matchPool: MatchPool, lineIdx:Int):Set[TkMatch] = {
    val sot = matchPool.input.linesOfTokens(lineIdx)
    val res = mutable.Set[TkMatch]()
    sot.indices.foreach { idx =>
      res ++= runAtLineFrom(matchPool, lineIdx, idx)
    }
    res.toSet
  }

  def runAtLineFrom(matchPool: MatchPool, lineIdx:Int, start:Int):Set[TkMatch]
}
