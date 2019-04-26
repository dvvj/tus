package org.ditw.demo1.matchers
import org.ditw.demo1.gndata.GNCntry.GNCntry

object TagHelper extends Serializable {

  private[demo1] val TmPrepIn = "_TmPrepIn"
  private[demo1] val TmLaLower = "_TmLaLower"
  private[demo1] val TmAlLower = "_TmAlLower"
  val TmGNBlacklist = "TmGNBlacklist"
  val TmGNWhitelist = "TmGNWhitelist"
  private[demo1] val TmGNBlocker = "_TmGNBlocker_"
  def tmTagGnBlocker(tag2Block:String):String = s"$TmGNBlocker$tag2Block"

  val TmAdm0 = "TmAdm0"
  private val AdmTmPfx = "TmAdm_"
  def admTmTag(cntry: GNCntry):String = AdmTmPfx + cntry
  private val _Adm1SubTmPfx = "TmAdm1Sub_"
  private val _DynamicTmPfx = "_DyAdm_"
  private[demo1] val CityAdm1Pfxs = IndexedSeq(_Adm1SubTmPfx, _DynamicTmPfx)
  def adm1SubEntTmTag(adm1Code:String):String = _Adm1SubTmPfx + adm1Code
  def admDynTag(admCode:String):String = _DynamicTmPfx + admCode
  private val _CountryTmPfx = "_CNTR_"
  def countryTag(cntry: String):String = _CountryTmPfx + cntry
  def countryTag(cntry: GNCntry):String = _CountryTmPfx + cntry
  private val _CityTmPfx = "_CITY_"
  def cityOfCountryTag(cntry: GNCntry):String = _CityTmPfx + cntry
  private[demo1] val _CityStatePfx = "_CITY_STATE_"
  def cityStateTag(adm1c: String):String = _CityStatePfx + adm1c
  private[demo1] val _StateCityPfx = "_STATE_CITY_"
  def stateCityTag(adm1c: String):String = _StateCityPfx + adm1c
  private[demo1] val _CityCountryPfx = "_CITY_CNTR_"
  def cityCountryTag(cntry: GNCntry):String = _CityCountryPfx + cntry
  private[demo1] val GNIdTagPfx = "GNId_"
  private[demo1] def GNIdTag(id:Long) = s"$GNIdTagPfx$id"

  private[demo1] val _CityAdmSeqPfx = "_CITY_ADMSEQ_"
  def cityAdmSeqTag(cntry: GNCntry):String = _CityAdmSeqPfx + cntry

}
