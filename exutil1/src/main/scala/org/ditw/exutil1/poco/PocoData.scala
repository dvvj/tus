package org.ditw.exutil1.poco
import org.ditw.extract.TXtr
import org.ditw.extract.TXtr.XtrPfx
import org.ditw.exutil1.extract.PocoXtrs.{pocoUS2GNid, pocoXtr4TagPfx, pocogbPfx2GNid}
import org.ditw.matcher.{TCompMatcher, TkMatch}

object PocoData extends Serializable {

  import org.ditw.matcher.CompMatchers._
  import org.ditw.matcher.CompMatcherNs._
  import org.ditw.matcher.TokenMatchers._
  private[exutil1] val CC_GB = "GB"
  private[exutil1] val CC_US = "US"
  private val PocoGB = new TPoco {
    // https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom
    // AA9A 9AA	WC postcode area; EC1–EC4, NW1W, SE1P, SW1	EC1A 1BB
    // A9A 9AA	E1W, N1C, N1P	W1A 0AX
    // A9 9AA	B, E, G, L, M, N, S, W	M1 1AE
    // A99 9AA	B33 8TH
    // AA9 9AA	All other postcodes	CR2 6XH
    // AA99 9AA	DN55 1PT
    private val regexOutward = regex("[A-Z]{1,2}[0-9]([0-9A-Z])?")
    private val regexInward = regex("[0-9][A-Z]{2}")

    override def genMatcher: TCompMatcher = {
      lng(
        IndexedSeq(
          byTm(regexOutward), byTm(regexInward)
        ),
        PocoTags.pocoTag(CC_GB)
      )
    }
    override def check(poco: String): Boolean = throw new RuntimeException("todo")
    override val xtr: TXtr[Long] = pocoXtr4TagPfx(
      pocogbPfx2GNid, PocoTags.pocoTag(PocoData.CC_GB)
    )
  }

  private val PocoUS = new TPoco {
    private val _regex = regex("\\d{5}([‑-]\\d{4})?")

    override def genMatcher: TCompMatcher = byTmT(_regex, PocoTags.pocoTag(CC_US))

    override val xtr: TXtr[Long] = pocoXtr4TagPfx(
      pocoUS2GNid, PocoTags.pocoTag(PocoData.CC_US)
    )

    override def check(poco: String): Boolean = throw new RuntimeException("todo")
  }

  val cc2Poco:Map[String, TPoco] = Map(
    CC_GB -> PocoGB,
    CC_US -> PocoUS
  )

  import PocoTags._
  val pocoXtr:TXtr[Long] = new XtrPfx[Long](_PocoCountryPfx) {
    override def extract(m: TkMatch):List[Long] = {
      val ccs = m.getTags.filter(_.startsWith(_PocoCountryPfx))
      if (ccs.nonEmpty) {
        if (ccs.size == 1) {
          val cc = ccs.head.substring(_PocoCountryPfx.length)
          cc2Poco(cc).xtr.extract(m.children.head)
        }
        else
          throw new RuntimeException(s"multiple country codes? ${ccs.mkString(",")}")
      }
      else
        throw new RuntimeException("No country code?")
    }
  }

}
