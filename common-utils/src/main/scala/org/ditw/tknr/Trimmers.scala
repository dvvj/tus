package org.ditw.tknr

import scala.util.matching.Regex

/**
  * Created by dev on 2018-10-26.
  */
object Trimmers {
  trait TTrimmer extends Serializable {
    def run(input: String): TrimResult
  }

  private[tknr] case class TrimResult(
    result: String,
    leftTrimmed: String,
    rightTrimmed: String
  ) {}

  private val EmptyStr = ""
  def nothingTrimmed(input: String): TrimResult =
    TrimResult(input, EmptyStr, EmptyStr)
  def allTrimmed(input: String): TrimResult =
    TrimResult(EmptyStr, input, EmptyStr)
  private[Trimmers] class TrimmerByChars(
    private val chars2Trim: Set[Char]
  ) extends TTrimmer {
    override def run(input: String): TrimResult = {
      var idx1 = 0
      while (idx1 < input.length && chars2Trim.contains(input.charAt(idx1))) {
        idx1 += 1
      }
      if (idx1 < input.length) {
        var idx2 = input.length - 1
        while (idx2 >= idx1 && chars2Trim.contains(input.charAt(idx2))) {
          idx2 -= 1
        }
        val left =
          if (idx1 == 0) EmptyStr
          else input.substring(0, idx1)
        val right =
          if (idx2 == input.length - 1) EmptyStr
          else input.substring(idx2 + 1)
        val res =
          if (idx1 == 0 && idx2 == input.length - 1)
            input
          else input.substring(idx1, idx2 + 1)
        TrimResult(res, left, right)
      } else
        allTrimmed(input)
    }
  }

  def byChars(chars2Trim: Iterable[Char]): TTrimmer =
    new TrimmerByChars(chars2Trim.toSet)

  private[Trimmers] class RegexTrimmer(
    private val condRegex: Regex,
    private val chars2Trim: Set[Character]
  ) extends TTrimmer {
    override def run(input: String): TrimResult = {
      throw new RuntimeException()
    }
  }
}
