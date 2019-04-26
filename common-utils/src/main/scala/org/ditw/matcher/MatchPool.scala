package org.ditw.matcher
import org.ditw.common.{Dict}
import org.ditw.tknr.TknrResult
import org.ditw.tknr.Tokenizers.TTokenizer

class MatchPool(
  val input: TknrResult
) extends Serializable {
  import MatchPool._
  import collection.mutable
  private var _map = mutable.Map[String, Set[TkMatch]]()
  private def getMap = _map.toSet
  def get(tag:String):Set[TkMatch] = _map.getOrElse(tag, EmptyMatches)
  def get(tags:Set[String]):Set[TkMatch] =
    tags.flatMap(_map.getOrElse(_, EmptyMatches))
  def add(tag:String, matches:Set[TkMatch]):Unit = {
    val existing = get(tag)
    _map.put(tag, existing ++ matches)
  }
  def add(tag:String, m:TkMatch):Unit = {
    val existing = get(tag)
    _map.put(tag, existing + m)
  }

  def remove(matches: Iterable[TkMatch]):Unit = {
    val toRemoveMap = matches.flatMap { m =>
      m.getTags.map(_ -> m)
    }.groupBy(_._1)
      .mapValues(_.map(_._2))
    toRemoveMap.foreach { kv =>
      val (tag, matches) = kv
      val existing = get(tag)
      update(
        tag,
        existing -- matches
      )
    }
  }

  def update(tag:String, matches:Set[TkMatch]):Unit = {
    _map.put(tag, matches)
  }

  def allTags():Iterable[String] = _map.keySet

  def allTagsPrefixedBy(pfx:String):Iterable[String] = {
    allTags.filter(_.startsWith(pfx))
  }
}

object MatchPool extends Serializable {
  private val EmptyMatches = Set[TkMatch]()
  def fromStr(str:String, tokenizer: TTokenizer, dict:Dict):MatchPool = {
    val tr = tokenizer.run(str, dict)
    new MatchPool(tr)
  }
}
