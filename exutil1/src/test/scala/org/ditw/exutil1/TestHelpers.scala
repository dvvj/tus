package org.ditw.exutil1
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.extract.XtrMgr
import org.ditw.exutil1.naen.NaEnData
import org.ditw.matcher.MatcherMgr
import org.ditw.tknr.TknrHelpers.TknrTextSeg

object TestHelpers extends Serializable {
  private[exutil1] val testTokenizer = TknrTextSeg()
  import NaEnData._
  private[exutil1] val testDict: Dict = InputHelpers.loadDict(
    vocab4NaEns(UsUnivColls) ++ vocab4NaEns(UsHosps)
  )

  val mmgr:MatcherMgr = {
    new MatcherMgr(
      List(tmUsUniv(testDict), tmUsHosp(testDict)),
      List(),
      List(),
      List()
    )
  }
}
