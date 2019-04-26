package org.ditw.demo1.matchers
import org.ditw.common.{Dict, InputHelpers, ResourceHelpers}
import org.ditw.common.TypeCommon.DictEntryKey
import org.ditw.demo1.extracts.Xtrs
import org.ditw.demo1.gndata.{GNSvc, TGNMap}
import org.ditw.extract.{TXtr, XtrMgr}
import org.ditw.matcher.TokenMatchers._
import org.ditw.matcher._

import scala.collection.mutable.ListBuffer

object Adm0Gen extends Serializable {
  import TagHelper._

  private val EmptyPairs = Iterable[(String, List[String])]()
  private val LookAroundSfxSet = Set(",")
  private val LookAroundSfxCounts_CityState = (3, 0)
  private val LookAroundSfxCounts_StateCity = (0, 3)
  private val LookAroundSfxCounts_CityCountry = (3, 0)
  def genMatcherExtractors(gnsvc:GNSvc, adm0:TGNMap, dict:Dict, containCityCountry:Boolean)
    :(List[TTkMatcher], List[TCompMatcher], List[TXtr[Long]], TPostProc) = {
    val pairs = adm0.admMap.toIndexedSeq
      .flatMap { p =>
        val admCode = p._1
        if (p._2.self.nonEmpty) {
          val admEnt = p._2.self.get
          admEnt.queryNames.map(_ -> List(admDynTag(admCode), GNIdTag(admEnt.gnid)))
        }
        else EmptyPairs
      }
    val name2Admc = pairs.groupBy(_._1)
      .mapValues(_.flatMap(_._2).toSet)

    // general tag for adms
    val admTag = admTmTag(adm0.countryCode)
    val tmAdm1s = TokenMatchers.ngramExtraTags(
      name2Admc,
      dict,
      admTag
      //addExtraAdmTags
    )

    val subAdmEnts = adm0.subAdms.map(adm0.admMap)
    val adm1NameIds = subAdmEnts.flatMap(c => c.self.map(cs => cs.queryNames.map(_ -> cs.gnid)).getOrElse(Set()))
    val adm1Names = adm1NameIds.map(_._1)
    val adm1Ids = adm1NameIds.map(_._2)

    // remove adm1 name->id pairs
    val adm2PlusMap = adm0.admNameMap.map { p =>
      val minusAdm1 = p._2.filter(p => !adm1Names.contains(p._1))
      val filtered = p._2.filter(p => adm1Names.contains(p._1))
        .flatMap { p =>
          val rmd = p._2.filter(!adm1Ids.contains(_))
          if (rmd.nonEmpty)
            Option(p._1 -> rmd)
          else None
        }
      p._1 -> (minusAdm1 ++ filtered)
    }

    val name2Adm1SubEnts = adm2PlusMap.map { p =>
      val adm1SubEntTag = adm1SubEntTmTag(p._1)
      ngramGNIds(p._2, dict, adm1SubEntTag)
    }

    val tms = tmAdm1s :: name2Adm1SubEnts.toList

    val xtrs = ListBuffer[TXtr[Long]]()
    val cms = adm0.admNameMap.keySet.flatMap { admc =>
      val csTag = cityStateTag(admc)
      val cs = CompMatcherNXs.sfxLookAroundByTag_R2L(
        LookAroundSfxSet, LookAroundSfxCounts_CityState,
        admDynTag(admc),
        adm1SubEntTmTag(admc),
        csTag
      )
      val scTag = stateCityTag(admc)
      val sc = CompMatcherNXs.sfxLookAroundByTag_R2L(
        LookAroundSfxSet, LookAroundSfxCounts_StateCity,
        admDynTag(admc),
        adm1SubEntTmTag(admc),
        scTag
      )
      List(cs, sc)
    }.toList
    val ct = countryTag(adm0.countryCode)
    val t:IndexedSeq[(String, IndexedSeq[Long])] = adm2PlusMap.toIndexedSeq.flatMap(_._2.toIndexedSeq)
    val cityMap = t
      .groupBy(_._1)
      .mapValues { maps =>
        val gnids = maps.flatMap(_._2).toIndexedSeq.distinct
        gnids
      }
    val cityTag = cityOfCountryTag(adm0.countryCode)
    val tmCity = ngramGNIds(
      cityMap,
      dict,
      cityOfCountryTag(adm0.countryCode)
    )
//    val cmCityCountry = CompMatcherNs.lngOfTags(
//      IndexedSeq(cityTag, ct),
//      cityCountryTag(adm0.countryCode)
//    )

    // todo: revisit this
//    val cmCityAdmSeq = GNMatchers.GNSeqByTags(
//      cityTag, admTag, gnsvc, cityAdmSeqTag(adm0.countryCode)
//    )

    val pprocBlockers = MatcherMgr.postProcBlocker_TagPfx(
      Map(
        _CityStatePfx -> Set(_CityCountryPfx, _StateCityPfx)
      )
    )

    val cmlst =
      if (containCityCountry) {
        val ccTag = cityCountryTag(adm0.countryCode)
        val cmCityCountry = CompMatcherNXs.sfxLookAroundByTag_R2L(
          LookAroundSfxSet, LookAroundSfxCounts_CityCountry,
          ct, cityTag,
          ccTag
        )
        xtrs += Xtrs.entXtr4Tag(ccTag)
        cmCityCountry :: cms
      }
      else {
        cms
      }

    (
      tmCity :: tms,
      cmlst,  // cmCityAdmSeq ::
      xtrs.toList, pprocBlockers
    )
  }


  private val addExtraAdmTag:TmMatchPProc[String] = (m, tag) => {
    val t = admDynTag(tag)
    m.addTag(t)
    m
  }
  private val addExtraAdmTags:TmMatchPProc[Set[String]] = (m, tags) => {
    val t = tags.map(admDynTag)
    m.addTags(t)
    m
  }
  private val addGNIdTags:TmMatchPProc[IndexedSeq[Long]] = (m, gnids) => {
    val tags = gnids.map(GNIdTag)
    m.addTags(tags, false)
    m
  }

  private def ngramGNIds(
    ngrams:Map[String, IndexedSeq[Long]],
    dict: Dict,
    tag:String,
    pproc:TmMatchPProc[IndexedSeq[Long]] = addGNIdTags
    ):TTkMatcher = {
    ngramT(ngrams, dict, tag, pproc)
    //    val encm:Map[Array[DictEntryKey], String] =
    //      ngrams.map(p => InputHelpers.splitVocabEntry(p._1).map(checkValidAndEnc(dict, _)) -> p._2)
    //    new TmNGramD(encm, pproc, Option(tag))
  }


//  def genCms(adm0:TGNMap, dict:Dict):Iterable[TCompMatcher] = {
//
//
//  }
  //def genCms()
}
