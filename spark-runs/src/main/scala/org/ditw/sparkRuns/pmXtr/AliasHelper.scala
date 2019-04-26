package org.ditw.sparkRuns.pmXtr
import scala.util.matching.Regex

object AliasHelper extends Serializable {

  trait TMatchRepl extends Serializable {
    def matchRepl(in:String):String
  }

  private val spaceSplitter = "\\s+".r
  private class MR1(
                     val centerStr:String,
                     val replStr:String,
                     val preTokens:Range,
                     val postTokens:Range,
                     val keepPost:Boolean = true
                   ) extends TMatchRepl {
    override def matchRepl(in:String):String = {
      val normed = spaceSplitter.split(in.toLowerCase()).filter(_.nonEmpty)
      var rem:String = ""
      val start = preTokens.find { i =>
        rem = normed.slice(i, normed.length).mkString(" ")
        rem.startsWith(centerStr)
      }
      if (start.nonEmpty) {
        val post = rem.substring(centerStr.length).trim
        val remTokens = spaceSplitter.split(post).filter(_.nonEmpty).length
        if (postTokens.contains(remTokens)) {
          if (keepPost)
            normed.mkString(" ").replace(centerStr, replStr)
          else {
            val pre = normed.slice(0, start.get).mkString(" ")
            s"$pre $replStr"
          }
        }
        else in
      }
      else in
    }
  }

  private val MR_xUnivSchoolOfY:TMatchRepl =
    new MR1("university school of", "school of", 1 to 2, 1 to 1)
  private val MR_xUnivCollegeOfY:TMatchRepl =
    new MR1("university college of", "college of", 1 to 2, 1 to 1)
  private val MR_xUnivAtY:TMatchRepl =
    new MR1("university at ", "university", 1 to 20, 1 to 2, false)

  private val MRList = List(
    MR_xUnivSchoolOfY, MR_xUnivCollegeOfY,
    MR_xUnivAtY //todo: entities end with bookstore/library
  )

  def univSchoolCollegeAlias(in:String):Option[String] = {
    val lower = in.toLowerCase()

    val it = MRList.iterator
    var found = false
    var repl = lower
    while (!found && it.hasNext) {
      repl = it.next.matchRepl(lower)
      if (repl != lower) {
        found = true
      }
    }

    if (repl != lower) {
      Option(repl)
    }
    else None
  }
}
