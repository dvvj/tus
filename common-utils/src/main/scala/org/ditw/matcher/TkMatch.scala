package org.ditw.matcher
import org.ditw.common.TkRange

import scala.collection.mutable.ListBuffer

class TkMatch private (
  val matchPool: MatchPool,
  val range: TkRange,
  val children: IndexedSeq[TkMatch]
) extends Serializable {
  import collection.mutable
  private val tags = mutable.Set[String]()
  def getTags:Set[String] = tags.toSet

  def addTag(ts:String):Unit = {
    tags += ts
    matchPool.add(ts, this)
  }

  def addTags(ts:Iterable[String],
              addToPool:Boolean = true
             ):Unit = {
    tags ++= ts
    if (addToPool)
      ts.foreach(matchPool.add(_, this))
  }

  override def hashCode(): Int = {
    (children.size << 16) + range.hashCode()
  }

  override def equals(obj: Any): Boolean = obj match {
    case m2:TkMatch => {
      range == m2.range && tags == m2.tags &&
        m2.children.size == children.size &&
        children.indices.forall(
          idx => m2.children(idx).range == children(idx).range
        ) // todo: check, not recursive now
    }
    case _ => false
  }

  override def toString: String = {
    val trTags = tags.toList.sorted.mkString(",")
    s"[$trTags]: $range"
  }

  def flatten:Iterable[TkMatch] = {
    val res = ListBuffer[TkMatch]()
    res += this
    children.foreach { c =>
      res ++= c.flatten
    }
    res.toList
  }
}

object TkMatch extends Serializable {

  val EmptyChildren:IndexedSeq[TkMatch] = IndexedSeq[TkMatch]()
  val EmptyTags:Set[String] = Set()
  def fromChildren(
    children:IndexedSeq[TkMatch],
    tags:Set[String] = EmptyTags
  ):TkMatch = {
    if (children.isEmpty)
      throw new IllegalArgumentException("Empty Children")
    val lineIndices = children.map(_.range.lineIdx).distinct
    if (lineIndices.size > 1)
      throw new IllegalArgumentException(
        s"Children in multiple lines: ${lineIndices.mkString(",")}"
      )
    val firstChildRange = children.head.range
    val range = TkRange(
      firstChildRange.input,
      firstChildRange.lineIdx,
      firstChildRange.start,
      children.last.range.end
    )
    new TkMatch(children.head.matchPool, range, children)
  }

  def oneChild(
    range: TkRange,
    child: TkMatch,
    tag:Option[String]
  ):TkMatch = {
    val res = new TkMatch(child.matchPool, range, IndexedSeq(child))
    if (tag.nonEmpty)
      res.addTag(tag.get)
    res
  }

  def noChild(matchPool: MatchPool, range:TkRange, tag:Option[String]):TkMatch = {
    val res = new TkMatch(matchPool, range, TkMatch.EmptyChildren)
    if (tag.nonEmpty)
      res.addTag(tag.get)
    res
  }

  def updateRange(orig:TkMatch, newRange:TkRange):TkMatch = {
    new TkMatch(orig.matchPool, newRange, orig.children)
  }

  def mergeByRange(matches:Iterable[TkMatch]):Set[TkMatch] = {
    val indexed = matches.toArray
    val idxToRemove = ListBuffer[Int]()
    indexed.indices.foreach { i =>
    val ri = indexed(i).range
      (i+1 until indexed.length).foreach { j =>
        val rj = indexed(j).range
        if (ri.covers(rj))
          idxToRemove += j
        else if (rj.covers(ri))
          idxToRemove += i
      }
    }
    val removeIdxSet = idxToRemove.toSet
    indexed.indices.filter(idx => !removeIdxSet.contains(idx))
      .map(indexed).toSet
  }
}