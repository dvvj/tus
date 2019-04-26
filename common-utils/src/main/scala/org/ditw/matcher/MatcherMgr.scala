package org.ditw.matcher
import scala.collection.mutable.ListBuffer

private[ditw] trait TPostProc extends Serializable {
  def run(matchPool: MatchPool):Unit
}

class MatcherMgr(
  val tms:Iterable[TTkMatcher],
  val tmPProcs:Iterable[TPostProc],
  val cms:Iterable[TCompMatcher],
  val postProcs:Iterable[TPostProc]
  //val blockTagMap:Map[String, Set[String]]
) extends Serializable {
  private def checkTags:Unit = {
    val allTags = tms.map(_.tag) ++ cms.map(_.tag)
    if (!allTags.forall(_.nonEmpty))
      throw new IllegalArgumentException(
        "Empty Tag Matcher found!"
      )
    val dupTags:Iterable[String] = allTags.flatten.groupBy(t => t)
      .mapValues(_.size)
      .filter(_._2 > 1)
      .keys
      .toList.sorted
    if (dupTags.nonEmpty)
      throw new IllegalArgumentException(
        s"Duplicate tag(s) found: [${dupTags.mkString(",")}]"
      )
  }
  checkTags

  private val tag2CmMap:Map[String, TCompMatcher] = {
    cms.map(m => m.tag.get -> m).toMap
  }
  private[matcher] val cmDepMap:Map[String, Set[String]] = {
    val depPairs = cms.flatMap(cm => cm.getRefTags().map(_ -> cm.tag.get))
    depPairs.groupBy(_._1)
      .mapValues(_.map(_._2).toSet)
      .toList.toMap // to make it serializable
  }
  import MatcherMgr._
  def run(matchPool: MatchPool):Unit = {
    tms.foreach { tm =>
      val matches = tm.run(matchPool)
      if (matches.nonEmpty)
        matchPool.add(tm.tag.get, matches)
    }

    tmPostproc(matchPool)

    import collection.mutable

    var remMatchers = mutable.Set[TCompMatcher]()
    remMatchers ++= cms

    val matchCache = mutable.Map[TCompMatcher, Set[TkMatch]]()
    while (remMatchers.nonEmpty) {
      val headMatcher = remMatchers.head
      remMatchers.remove(headMatcher)

      val currMatches = headMatcher.run(matchPool)
      val hasUpdates = currMatches != matchCache.getOrElse(headMatcher, EmptyMatches)
      if (hasUpdates) {
        matchPool.update(headMatcher.tag.get, currMatches)
        val affectedCmTags = cmDepMap.getOrElse(headMatcher.tag.get, EmptyDepCmTags)
//        if (headMatcher.tag.get == "__TagCmXDept") {
//          println(s"\tadding: $affectedCmTags")
//          println(s"\t$currMatches")
//        }
        remMatchers ++= affectedCmTags.map(tag2CmMap)
        matchCache.put(headMatcher, currMatches)
      }
    }

    postproc(matchPool)
  }

  private def runPostProcs(pprocs:Iterable[TPostProc], matchPool: MatchPool):Unit = {
    pprocs.foreach { pp =>
      pp.run(matchPool)
    }
  }
  private def postproc(matchPool: MatchPool):Unit = {
    runPostProcs(postProcs, matchPool)
  }
  private def tmPostproc(matchPool: MatchPool):Unit = {
    runPostProcs(tmPProcs, matchPool)
  }
}

object MatcherMgr extends Serializable {
  private val EmptyMatches = Set[TkMatch]()
  private val EmptyDepCmTags = Set[String]()

  private[ditw] def postProcPrioList(procList:List[TPostProc]):TPostProc = new TPostProc {
    override def run(matchPool: MatchPool): Unit = {
      procList.foreach(_.run(matchPool))
    }
  }

  private val EmptyTagWhiteList = Set[String]()
  private[ditw] def postProcBlocker(
    blockTagMap:Map[String, Set[String]],
    tagsWhitelist:Set[String] = EmptyTagWhiteList
  ):TPostProc = new TPostProc {
    override def run(matchPool: MatchPool): Unit = {
      val toRemoveList = ListBuffer[TkMatch]()
      val whiteListRanges = matchPool.get(tagsWhitelist).map(_.range)
      blockTagMap.foreach { kv =>
        val (blockerTag, blockeeTags) = kv
        val blockerRanges = matchPool.get(blockerTag).map(_.range)

        val blockees = matchPool.get(blockeeTags)

        blockees.foreach { blockee =>
          if (!whiteListRanges.contains(blockee.range) &&
            blockerRanges.exists(_.overlap(blockee.range))) {
            toRemoveList += blockee
          }
        }
      }

      matchPool.remove(toRemoveList)
    }
  }

  private[ditw] def postProcBlocker_TagPfx(blockTagMap:Map[String, Set[String]]):TPostProc = new TPostProc {
    override def run(matchPool: MatchPool): Unit = {
      val toRemoveList = ListBuffer[TkMatch]()
      blockTagMap.foreach { kv =>
        val (blockerTagPfx, blockeeTagPfxs) = kv
        val blockerTags = matchPool.allTags()
          .filter(_.startsWith(blockerTagPfx))
          .toSet
        val blockeeTags = matchPool.allTags()
          .filter(blockeeTag => blockeeTagPfxs.exists(blockeeTag.startsWith))
          .toSet
        val blockerRanges = matchPool.get(blockerTags).map(_.range)

        val blockees = matchPool.get(blockeeTags)

        blockees.foreach { blockee =>
          if (blockerRanges.exists(_.overlap(blockee.range))) {
            toRemoveList += blockee
          }
        }
      }

      matchPool.remove(toRemoveList)
    }
  }

  private[ditw] def postProcOverride(
    overrideMap:Map[String, String]
  ):TPostProc = new TPostProc {
    override def run(matchPool: MatchPool): Unit = {
      val overrideMergedMap = overrideMap.map { kv =>
        val overrideTag = kv._1
        val matches = matchPool.get(overrideTag)
        val merged = TkMatch.mergeByRange(matches)
        merged.foreach { m => m.addTag(kv._2) }
        overrideTag -> merged
      }

      val toRemoveList = ListBuffer[TkMatch]()
      overrideMap.foreach { kv =>
        val (overrideTag, toOverrideTag) = kv
        val overrideRanges = overrideMergedMap(overrideTag).map(_.range)

        val toOverride = matchPool.get(toOverrideTag)

        toOverride.foreach { toOv =>
          if (overrideRanges.exists(_.overlap(toOv.range))) {
            toRemoveList += toOv
          }
        }
      }

      matchPool.remove(toRemoveList)
//      println(s"\t$overrideMergedMap")
      overrideMap.foreach { kv =>
        val (overrideTag, toOverrideTag) = kv
        val overrideMatches = overrideMergedMap(overrideTag)
        if (overrideMatches.nonEmpty)
          matchPool.add(toOverrideTag, overrideMatches)
      }

    }
  }
}