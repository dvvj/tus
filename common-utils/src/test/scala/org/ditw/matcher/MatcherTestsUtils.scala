package org.ditw.matcher
import org.ditw.tknr.TestHelpers.{dict, testTokenizer}

object MatcherTestsUtils {

  private[matcher] def runCms(
    tms:List[TTkMatcher],
    cms:List[TCompMatcher],
    inStr:String,
    keys:Set[String]
  ) = {
    val mmgr = new MatcherMgr(tms, List(), cms, List())
    val matchPool:MatchPool = MatchPool.fromStr(inStr, testTokenizer, dict)
    mmgr.run(matchPool)
    val res = keys
      .map(k => k -> matchPool.get(k))
      .toMap
    val res2Ranges = res.mapValues { ms =>
      ms.map(_.range)
    }

    matchPool -> res2Ranges
  }

}
