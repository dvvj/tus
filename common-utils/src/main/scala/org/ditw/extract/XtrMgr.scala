package org.ditw.extract
import org.ditw.common.TkRange
import org.ditw.matcher.{MatchPool, TkMatch}

class XtrMgr[R] private (
  private val xtrs:List[TXtr[R]]
) extends Serializable {

  def run(matchPool: MatchPool):Map[TkRange, List[R]] = {
    xtrs.flatMap(_.extractAll(matchPool).toList)
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2).toList)
  }
}

object XtrMgr extends Serializable {

  def create[R](xtrs:List[TXtr[R]]):XtrMgr[R] = new XtrMgr(xtrs)
}
