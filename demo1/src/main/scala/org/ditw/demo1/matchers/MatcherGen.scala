package org.ditw.demo1.matchers
import org.ditw.common.{Dict, InputHelpers, ResourceHelpers}
import org.ditw.demo1.extracts.Xtrs
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.{GNCntry, GNSvc, TGNMap}
import org.ditw.demo1.matchers.Adm0Gen.{LookAroundSfxCounts_CityCountry, LookAroundSfxSet}
import org.ditw.demo1.matchers.TagHelper.countryTag
import org.ditw.extract.{TXtr, XtrMgr}
import org.ditw.exutil1.poco.{PocoData, PocoTags}
import org.ditw.matcher._

import scala.collection.mutable.ListBuffer

object MatcherGen extends Serializable {
  import InputHelpers._

  private[demo1] val _gnBlackList = ResourceHelpers.loadStrs("/gn_blacklist.txt").toSet
  private[demo1] val _gnWhiteList = ResourceHelpers.loadStrs("/gn_whitelist.txt").toSet
  private[demo1] val _gnBlockers:Array[GNBlocker] = ResourceHelpers.load("/gnbl_map.json", GNBlocker.load)

  private val extraVocabs = List(
    _gnBlackList, _gnWhiteList, _gnBlockers.flatMap(_.blockers.toIterable).toIterable
  )

  def wordsFromGNSvc(gnsvc: GNSvc
                    ):Iterable[Iterable[String]] = {
    val adm0s = gnsvc._cntryMap.values
    val keys = adm0s.flatMap(_.admNameMap.values.flatMap(_.keySet))
    val adm0Names = adm0s.flatMap(_.self.get.queryNames)
    val words = splitVocabEntries(keys.toSet ++ adm0Names ++ extraVocabs.flatten)
      .map(_.toIndexedSeq)
    words
  }

  def loadDict(
    gnsvc: GNSvc
  ):Dict = {
    InputHelpers.loadDict(wordsFromGNSvc(gnsvc))
  }

  import TagHelper._

  private val _tmInLower = TokenMatchers.regex("in", Option(TmPrepIn))
  private val _tmLaLower = TokenMatchers.regex("[lL]a", Option(TmLaLower))
  private val _tmAlLower = TokenMatchers.regex("[aA]l", Option(TmAlLower))

  private val LookAroundSfxSet = Set(",")
  private val LookAroundSfxCounts_PocoCountry = (3, 0)
  def gen(
           gnsvc:GNSvc,
           dict:Dict,
           ccCntries:Set[GNCntry],
           extras:Option[(Iterable[TTkMatcher], Iterable[TCompMatcher], Iterable[TPostProc])] = None
         ): (MatcherMgr, XtrMgr[Long]) = {
    val tmlst = ListBuffer[TTkMatcher]()
    val cmlst = ListBuffer[TCompMatcher]()
    val pproclst = ListBuffer[TPostProc]()

    val tmGNBlackList = TokenMatchers.ngramT(
      splitVocabEntries(_gnBlackList),
      dict,
      TmGNBlacklist
    )
    val tmGNWhiteList = TokenMatchers.ngramT(
      splitVocabEntries(_gnWhiteList),
      dict,
      TmGNWhitelist
    )
    val tmGNBlockerMap = _gnBlockers.map { gnbl =>
      val blockerTag = tmTagGnBlocker(gnbl.tag2Block)
      tmlst += TokenMatchers.ngramT(
        splitVocabEntries(gnbl.blockers.toSet),
        dict,
        blockerTag
      )
      blockerTag -> Set(gnbl.tag2Block)
    }.toMap
    tmlst += tmGNBlackList
    tmlst += tmGNWhiteList

    pproclst += MatcherMgr.postProcBlocker(tmGNBlockerMap)

    val adm0s: Iterable[TGNMap] = gnsvc._cntryMap.values
    val adm0Name2Tag = adm0s.flatMap { adm0 =>
      val ent = adm0.self.get
      ent.queryNames.map(_ -> TagHelper.countryTag(adm0.countryCode))
    }.toMap
    val tmAdm0 = TokenMatchers.ngramExtraTag(
      adm0Name2Tag,
      dict,
      TagHelper.TmAdm0
    )
    tmlst += tmAdm0

    val xtrlst = ListBuffer[TXtr[Long]]()
    import collection.mutable
    val tmBlTargets = mutable.Set[String]()
    adm0s.foreach { adm0 =>
      val (tms, cms, xtrs, pproc) = Adm0Gen.genMatcherExtractors(
        gnsvc, adm0, dict,
        ccCntries.contains(adm0.countryCode)
      )
      tmBlTargets ++= tms.flatMap(_.tag) // black list blocks all tms
      tmlst ++= tms
      cmlst ++= cms
      xtrlst ++= xtrs
      pproclst += pproc
    }

    tmlst += _tmInLower
    tmlst += _tmLaLower
    tmlst += _tmAlLower
    val tmPProc = MatcherMgr.postProcBlocker(
      tmGNBlockerMap ++ Map(
        TmGNBlacklist -> tmBlTargets.toSet
        //,countryTag(GNCntry.CN) -> Set(countryTag(GNCntry.PR))
        ,_tmInLower.tag.get -> Set(admDynTag("US_IN"))
        ,_tmLaLower.tag.get -> Set(admDynTag("US_LA"))
        ,_tmAlLower.tag.get -> Set(admDynTag("US_AL"))
      ),
      Set(TmGNWhitelist)
    )

    xtrlst += Xtrs.entXtr4TagPfx(_CityStatePfx)
    xtrlst += Xtrs.entXtr4TagPfxLast(_StateCityPfx)

    //tmlst ++= PocoData.cc2Poco.values.map(_.genMatcher)
    cmlst ++= PocoData.cc2Poco.map { p =>
      val (cc, poco) = p
      val mtr = poco.genMatcher
      tmlst += mtr
      val ct = countryTag(cc)
      CompMatcherNXs.sfxLookAroundByTag_R2L(
        LookAroundSfxSet, LookAroundSfxCounts_PocoCountry,
        ct, mtr.tag.get,
        PocoTags.pocoCountryTag(cc)
      )
    }
    xtrlst += PocoData.pocoXtr
    // xtrlst += Xtrs.entXtr4TagPfx(_CityCountryPfx)
    // xtrlst += Xtrs.entXtrFirst4TagPfx(gnsvc, _CityAdmSeqPfx)
    if (extras.nonEmpty) {
      val ex = extras.get
      tmlst ++= ex._1
      cmlst ++= ex._2
      pproclst ++= ex._3
    }

    new MatcherMgr(
      tmlst.toList,
      List(tmPProc),
      cmlst.toList,
      pproclst.toList
    ) -> XtrMgr.create(xtrlst.toList)
  }


}
