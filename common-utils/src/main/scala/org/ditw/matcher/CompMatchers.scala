package org.ditw.matcher

object CompMatchers extends Serializable {

  private [ditw] trait TDefRunAtLineFrom extends TCompMatcher {
    override def runAtLineFrom(
      matchPool: MatchPool,
      lineIdx: Int,
      start: Int
    ):Set[TkMatch] = {
      val all = run(matchPool)
      all.filter { m =>
        m.range.lineIdx == lineIdx && m.range.start == start
      }
    }
  }

  // -------------- by-tag
  private[matcher] class CmByTags(
    private val tagsToFind:Set[String],
    val tag:Option[String]
  ) extends TCompMatcher {
    override def runAtLineFrom(
      matchPool: MatchPool,
      lineIdx: Int,
      start: Int): Set[TkMatch] = {
      val candidates = matchPool.get(tagsToFind)
      candidates.filter(m => m.range.lineIdx == lineIdx && m.range.start == start)
    }

    // optim:
    override def run(matchPool: MatchPool)
      : Set[TkMatch] = {
      tagsToFind.flatMap(matchPool.get)
    }

    override def runAtLine(
      matchPool: MatchPool,
      lineIdx: Int
    ): Set[TkMatch] = {
      val candidates = tagsToFind.flatMap(matchPool.get)
      candidates.filter(m => m.range.lineIdx == lineIdx)
    }

    override def getRefTags()
      : Set[String] = tagsToFind
  }

  def byTags(tagsToFind:Set[String], tag:String):TCompMatcher = {
    new CmByTags(tagsToFind, Option(tag))
  }
  def byTags(tagsToFind:Set[String]):TCompMatcher = {
    new CmByTags(tagsToFind, None)
  }
  def byTag(tagToFind:String, tag:String):TCompMatcher = {
    new CmByTags(Set(tagToFind), Option(tag))
  }
  def byTag(tagToFind:String):TCompMatcher = {
    new CmByTags(Set(tagToFind), None)
  }

  private val EmptyRefTags = Set[String]()

  // -------------- from token matcher
  private[matcher] class CmByTm(
    private val tm:TTkMatcher,
    val tag:Option[String]
  ) extends TCompMatcher {
    override def getRefTags(): Set[String] = EmptyRefTags

    override def runAtLineFrom(
      matchPool: MatchPool,
      lineIdx: Int,
      start: Int): Set[TkMatch] = {
      val matches = tm.runAtLineFrom(matchPool, lineIdx, start)
      if (tag.nonEmpty) {
        matches.foreach(m => m.addTag(tag.get))
      }
      matches
    }
  }

  def byTm(tm:TTkMatcher):TCompMatcher = {
    new CmByTm(tm, None)
  }
  def byTmT(tm:TTkMatcher, tag:String):TCompMatcher = {
    new CmByTm(tm, Option(tag))
  }
}
