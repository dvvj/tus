package org.ditw.textSeg.common
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.matcher._
import org.ditw.textSeg.catSegMatchers.Cat1SegMatchers
import org.ditw.textSeg.common.CatSegMatchers.TSegMatchers4Cat
import org.ditw.tknr.Tokenizers.TTokenizer

object AllCatMatchers extends Serializable {

  import org.ditw.matcher.TokenMatchers._
  import Tags._
  import org.ditw.tknr.TknrHelpers._

  import AssiMatchers._
  import Vocabs._
  def mmgrFrom(
    dict: Dict,
    catSegMatchers:TSegMatchers4Cat*
  ):MatcherMgr = {
    val (tms, cms, postprocs) = segMatchersFrom(dict, catSegMatchers)
    new MatcherMgr(
      tms,
      List(),
      cms,
      postprocs
    )
  }

  def segMatchersFrom(dict: Dict, catSegMatchers:Seq[TSegMatchers4Cat])
    :(Iterable[TTkMatcher], Iterable[TCompMatcher], Iterable[TPostProc]) = {
    (
      _ExtraTms(dict) ++ catSegMatchers.flatMap(_.tms),
      _ExtraCms ++ catSegMatchers.flatMap(_.cms),
      catSegMatchers.map(_.postproc)
    )
  }

  def run(
    mmgr:MatcherMgr,
    inStr:String,
    dict: Dict,
    tokenizer:TTokenizer
  ): MatchPool = {
    val matchPool = MatchPool.fromStr(inStr, tokenizer, dict)
    mmgr.run(matchPool)
    matchPool
  }

}
