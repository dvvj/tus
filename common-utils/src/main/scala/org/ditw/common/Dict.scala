package org.ditw.common

import java.lang.{Integer => JavaInt}
import TypeCommon._
class Dict(
  private val map:Map[String, DictEntryKey]
) extends Serializable {
  import Dict._
  private val revMap:Map[DictEntryKey, String] = map.map(p => p._2 -> p._1)
  val size:Int = map.size
  def contains(w:String):Boolean = map.contains(w.toLowerCase())
  def enc(w:String):DictEntryKey = map.getOrElse(w.toLowerCase(), UNKNOWN)
  def dec(c:DictEntryKey):String = revMap(c)
}

object Dict extends Serializable {
  val UNKNOWN = Int.MinValue
}