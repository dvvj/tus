package org.ditw.tknr

import org.ditw.common.{Dict, GenUtils}
import org.ditw.tknr.Trimmers.TTrimmer

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/**
  * Created by dev on 2018-10-26.
  */
object Tokenizers extends Serializable {
  trait TTokenizer extends Serializable {
    def run(input: String, dict:Dict): TknrResult
  }

  trait TTokenSplitter extends Serializable {
    def split(input: String): IndexedSeq[String]
  }

  val EmptyTokenStrs = IndexedSeq[String]()
  case class TokenSplitterCond(
    _condRegex: String,
    _tokenSplitter: String,
    splitterStrsToKeep:Set[String]
  ) extends TTokenSplitter {
    private val condRegex: Regex = _condRegex.r
    private val tokenSplitter: Regex = _tokenSplitter.r
    def split(input: String): IndexedSeq[String] = {
      if (canSplit(input)) {
        val parts = tokenSplitter.split(input)
        val res = ListBuffer[String]()
        var start = 0
        parts.foreach { p =>
          val s = input.indexOf(p, start)
          if (s > start) {
            val pfx = input.substring(start, s).trim
            if (!pfx.isEmpty && splitterStrsToKeep.contains(pfx.toLowerCase()))
              res += pfx
          }
          res += p
          start = s + p.length
        }
        val rem = input.substring(start)
        if (splitterStrsToKeep.contains(rem.toLowerCase()))
          res += rem
        res.toIndexedSeq
      } else
        throw new RuntimeException("Error!")
    }

    def canSplit(input: String): Boolean =
      condRegex.pattern.matcher(input).matches()
  }

  case class RegexTokenSplitter(_tokenSplitter: String) extends TTokenSplitter {
    private val splitter = _tokenSplitter.r
    override def split(input: String): IndexedSeq[String] = {
      splitter.split(input)
    }
  }

  private def toLowerSortByLenDesc(in:Iterable[String]):List[String] =
    in.map(_.toLowerCase()).toList.sortBy(_.length)(Ordering[Int].reverse)

  case class TokenizerSettings(
    _lineSplitter: String,
    private val _specialTokens: Set[String],
    _tokenSplitter: String,
    private val _tokenSplitter2Keep: Set[String],
    tokenSplitterCond: List[TokenSplitterCond],
    _tokenTrimmer: TTrimmer
  ) {
    private[Tokenizers] val specialTokens = toLowerSortByLenDesc(_specialTokens)
    private[Tokenizers] val tokenSplitter2Keep = toLowerSortByLenDesc(_tokenSplitter2Keep)
    private[Tokenizers] val lineSplitter: Regex = _lineSplitter.r
    private[Tokenizers] val tokenSplitter: TTokenSplitter =
      RegexTokenSplitter(_tokenSplitter)
  }

  case class TmpToken(c:String, divideble:Boolean)

  private def checkStandalone(line:String, idx:Int, len:Int):Boolean = {
    assert(idx >= 0 && idx + len <= line.length)
    val leftCheck =
      idx == 0 || line(idx-1).isSpaceChar
    val rightCheck =
      idx + len == line.length || line(idx+len).isSpaceChar
    leftCheck && rightCheck
  }

  private def _splitBy(inToken:TmpToken, splitter:String, standalone:Boolean):IndexedSeq[TmpToken] = {
    if (inToken.divideble) {
      val tmpTokens = ListBuffer[TmpToken]()
      var findStart = 0
      var segStart = findStart
      val inStr = inToken.c
      val inLower = GenUtils.removeAccents(inStr.toLowerCase())
      while (findStart < inLower.length) {
        var idx = inLower.indexOf(splitter, findStart)
        if (idx >= 0) {
          findStart = idx + splitter.length
          if (!standalone || checkStandalone(inLower, idx, splitter.length)) {
            val pre = inStr.substring(segStart, idx).trim
            if (pre.nonEmpty)
              tmpTokens += TmpToken(pre, true)
            tmpTokens += TmpToken(inStr.substring(idx, idx+splitter.length), false)
            segStart = idx+splitter.length
          }
        }
        else {
          val t = inStr.substring(segStart).trim
          tmpTokens += TmpToken(t, true)
          findStart = inLower.length
        }
      }
      tmpTokens.toIndexedSeq
    }
    else {
      IndexedSeq(inToken)
    }
  }

  private def splitBy(inStr:String, splitters:List[String], standalone:Boolean):IndexedSeq[TmpToken] = {
    var tts = IndexedSeq(TmpToken(inStr, true))

    splitters.foreach { splitter =>
      tts = tts.flatMap(tt => _splitBy(tt, splitter, standalone))
    }

    tts
  }

  private[Tokenizers] class Tokenizer(private val _settings: TokenizerSettings)
    extends TTokenizer {
    override def run(input: String, dict:Dict): TknrResult = {
      val linesOfTokens = _settings.lineSplitter.split(input).map { l =>
        val line = l.trim
        val tmpTokens = splitBy(line, _settings.specialTokens, true)
        val tokens = tmpTokens.flatMap { tt =>
          if (tt.divideble) {
            val split1 = _settings.tokenSplitter.split(tt.c)
              .flatMap(t => splitBy(t, _settings.tokenSplitter2Keep, false).map(_.c))

            split1.flatMap { t =>
              var processed = false;
              val it = _settings.tokenSplitterCond.iterator
              var res = EmptyTokenStrs
              while (!processed && it.hasNext) {
                val condSplitter = it.next()
                if (condSplitter.canSplit(t)) {
                  res = condSplitter.split(t)
                  processed = true
                }
              }
              if (!processed)
                res = IndexedSeq(t)
              res.filter(!_.isEmpty)
            }
          }
          else {
            List(tt.c)
          }
        }

        val resTokens = tokens.indices.map { idx =>
          val trimRes = _settings._tokenTrimmer.run(tokens(idx))
          new Token(
//            lineResult,
            idx,
            trimRes.result,
            trimRes.leftTrimmed,
            trimRes.rightTrimmed)
        }
        val lineOfTokens = new SeqOfTokens(line, tokens, resTokens)
        //lineResult._setTokens(resTokens)
        lineOfTokens
      }

      new TknrResult(input, dict, linesOfTokens)
    }
  }

  def load(settings: TokenizerSettings): TTokenizer = new Tokenizer(settings)
}
