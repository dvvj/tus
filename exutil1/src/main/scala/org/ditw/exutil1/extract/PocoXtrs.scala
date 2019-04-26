package org.ditw.exutil1.extract
import org.ditw.common.ResourceHelpers
import org.ditw.extract.TXtr
import org.ditw.extract.TXtr.XtrPfx
import org.ditw.exutil1.poco.{PoPfxGB, PocoData, PocoTags, PocoUS}
import org.ditw.matcher.TkMatch

object PocoXtrs extends Serializable {

  private val pocogbPfxs = ResourceHelpers.load("/poco/gb_pfx.json", PoPfxGB.fromJson)
  private[exutil1] val pocogbPfx2GNid = pocogbPfxs.flatMap { pocogbPfx =>
    pocogbPfx.gnid2Pfxs.flatMap { p =>
      val id = p._1
      val pfxs = p._2.toSet
      val t = pfxs.map(pfx => pfx.replaceAllLiterally(" ", ""))
      t.map(_ -> id)
    }
  }.toMap

  private type Match2QueryStr = (String, Int) => String

  private[exutil1] def pocoXtr4TagPfx(
    pfx2IdMap:Map[String, Long],
    tagPfx:String,
    match2Query: Option[Match2QueryStr] = None
  ):TXtr[Long] = new XtrPfx[Long](tagPfx) {
    private val pfxRange:Range = {
      val t = pfx2IdMap.keySet.map(_.length)
      t.min to t.max
    }
    override def extract(m: TkMatch)
    : List[Long] = {
      // todo: generalize to other countries
      val all = m.range.str.replace(" ", "")
      val maxLen = math.min(all.length, pfxRange.end)
      var found = false
      var currLen = maxLen
      var res:List[Long] = Nil
      while (!found && currLen >= pfxRange.start) {
        val currPfx =
          if (match2Query.isEmpty) all.substring(0, currLen)
          else match2Query.get(all, currLen)
        if (pfx2IdMap.contains(currPfx)) {
          found = true
          res = List(pfx2IdMap(currPfx))
        }
        currLen -= 1
      }
      res
    }
  }


  private val pocoUS = ResourceHelpers.load("/poco/us.json", PocoUS.fromJson)
  private[exutil1] val pocoUS2GNid = pocoUS.flatMap { poco =>
    poco.gnid2Poco.flatMap { p =>
      val id = p._1
      val pfxs = p._2.toSet
      pfxs.map(_ -> id)
    }
  }.toMap

}
