package org.ditw.exutil1.poco
import org.ditw.extract.TXtr
import org.ditw.matcher.TCompMatcher

trait TPoco extends Serializable {
  def genMatcher: TCompMatcher
  val xtr:TXtr[Long]
  def check(poco:String):Boolean
}
