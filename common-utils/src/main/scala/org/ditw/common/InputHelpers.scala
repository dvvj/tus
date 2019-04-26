package org.ditw.common

import TypeCommon._
import org.ditw.tknr.TknrHelpers
object InputHelpers extends Serializable {

  def loadDict0(words:Iterable[Iterable[String]]):Dict = {
    val wordSeq = words.flatMap(s => s.map(_.toLowerCase()))
      .toSet
      .toIndexedSeq
    val m:Map[String, DictEntryKey] = wordSeq.sorted.indices
      .map(idx => wordSeq(idx) -> idx.asInstanceOf[DictEntryKey])
      .toMap
    new Dict(m)
  }

  def loadDict(words:Iterable[String]*):Dict = {
    loadDict0(words)
  }

  def loadDict(words:Iterable[Iterable[String]]):Dict = {
    loadDict0(words)
  }

  private val SpaceSplitter = "\\s+".r
  def splitVocabToSet(phrases:Iterable[String]):Set[String] = {
    phrases.flatMap(splitVocabEntry).map(_.toLowerCase()).toSet
  }

  private val punctCharSet = TknrHelpers.PunctChars.toSet
  def splitVocabEntries(phrases:Set[String]):Set[Array[String]] = {
    phrases.map(splitVocabEntry)
  }

  import TknrHelpers._
  def fixManualPhrases(phrases:Iterable[String]):Iterable[String] = {
    phrases.map { phrase =>
      val idxs = phrase.indices.filter(idx => DashSlashSet.contains(phrase(idx).toString))
      if (idxs.isEmpty)
        phrase
      else {
        var res = phrase
        var offset = 0
        idxs.foreach { idx =>
          var offIdx = offset + idx
          if (offIdx > 0 && !Character.isSpaceChar(res(offIdx-1))) {
            res = res.substring(0, offIdx) + ' '  + res.substring(offIdx)
            offset += 1
            offIdx = offset + idx
          }
          if (offIdx < res.length-1 && !Character.isSpaceChar(res(offIdx+1))) {
            res = res.substring(0, offIdx+1) + ' '  + res.substring(offIdx+1)
            offset += 1
          }
        }
        res
      }
    }

  }

  def splitVocabEntry(phrase:String):Array[String] = {
//    if (phrase == null)
//      println("ok")
    val tokenSeq = SpaceSplitter.split(phrase)
    tokenSeq.flatMap { token =>
      var t = token
      while (t.nonEmpty && punctCharSet.contains(t.last)) {
        t = t.substring(0, t.length - 1)
      }
      if (t.nonEmpty)
        Option(t)
      else None
    }
  }

}
