package org.ditw.exutil1.poco

object PocoTags extends Serializable {
  val PocoCountryPfx:String = "_PocoPfx_"
  def pocoTag(cc:String) = s"$PocoCountryPfx$cc"

  private[exutil1] val _PocoCountryPfx = "_POCO_CNTR_"
  def pocoCountryTag(cntry: String):String = _PocoCountryPfx + cntry

}
