package org.ditw.sparkRuns
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{Dict, InputHelpers, TkRange}
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.{GNEnt, GNSvc}
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.demo1.matchers.MatcherGen
import org.ditw.extract.XtrMgr
import org.ditw.exutil1.naen.NaEnData._
import org.ditw.exutil1.naen.{NaEn, NaEnData, SrcCsvMeta}
import org.ditw.exutil1.poco.PocoUS
import org.ditw.matcher.{MatchPool, MatcherMgr}
import org.ditw.sparkRuns.CommonUtils.GNMmgr
import org.ditw.sparkRuns.csvXtr.EntXtrUtils
import org.ditw.sparkRuns.csvXtr.UtilsEntCsv1.{Pfx2Replace, processName}
import org.ditw.textSeg.catSegMatchers.Cat2SegMatchers
import org.ditw.textSeg.common.{AllCatMatchers, Vocabs}
import org.ditw.tknr.TknrHelpers
import org.ditw.tknr.Tokenizers.TTokenizer

object CommonUtils extends Serializable {

  private def genMMgr(
    gnsvc: GNSvc, dict: Dict, ccms:Set[GNCntry],
    extraNaEns:Map[String, Array[NaEn]]
  ):(MatcherMgr, XtrMgr[Long]) = {
    val exMatchers = AllCatMatchers.segMatchersFrom(
      dict,
      Seq(Cat2SegMatchers.segMatchers(dict))
    )
    val exNaEnsTms = extraNaEns.map(p => NaEnData.tm4NaEns(p._2, dict, p._1))
    MatcherGen.gen(
      gnsvc, dict, ccms,
      Option(
        exMatchers._1 ++ exNaEnsTms,
        exMatchers._2,
        exMatchers._3
      )
    )
  }

  case class GNMmgr(
    tknr:TTokenizer,
    svc:GNSvc,
    dict:Dict,
    mmgr:MatcherMgr,
    xtrMgr: XtrMgr[Long],
    naEntDataMap:Map[Long, NaEn]
  )

  private[sparkRuns] def loadGNMmgr(
    ccs:Set[GNCntry],
    ccms:Set[GNCntry], // countries using
    spark:SparkContext,
    gnPath:String,
    extraNaEns:Map[String, Array[NaEn]] = Map()
  ):GNMmgr = {
    _loadGNMmgr(
      ccs, ccms, spark,
      spark.textFile(gnPath),
      extraNaEns
    )
  }

  private[sparkRuns] def loadGNSvc(ccs:Set[GNCntry], gndata:RDD[String], popuReq:Boolean = false):GNSvc = {
    val gnLines = gndata
      .map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val svc =
      if (popuReq) GNSvc.loadDef(gnLines, ccs)
      else GNSvc.loadNoPopuReq(gnLines, ccs)
    svc
  }


  private[sparkRuns] def _loadGNMmgr(
    ccs:Set[GNCntry],
    ccms:Set[GNCntry], // countries using
    spark:SparkContext,
    gndata:RDD[String],
    extraNaEns:Map[String, Array[NaEn]],
    popuReq:Boolean = false
  ):GNMmgr = {
    val svc =
      loadGNSvc(ccs, gndata, popuReq)
    val extraVocabs = extraNaEns.values.flatMap(NaEnData.vocab4NaEns)
    val dict = loadDict(svc, extraVocabs)
    val (mmgr, xtrMgr) = genMMgr(svc, dict, ccms, extraNaEns)
    val tknr = TknrHelpers.TknrTextSeg()
    val naEnMap = extraNaEns.values.flatten.map(e => e.neid -> e).toMap
    GNMmgr(tknr, svc, dict, mmgr, xtrMgr, naEnMap)
  }
  private[sparkRuns] def runStr(
    str:String,
    tknr:TTokenizer,
    dict: Dict,
    mmgr: MatcherMgr,
    svc:GNSvc,
    xtrMgr: XtrMgr[Long],
    wholeStrMatch:Boolean
  ):Map[TkRange, List[GNEnt]] = {
    val mp = MatchPool.fromStr(str, tknr, dict)
    mmgr.run(mp)
//    if (str == "SAN JUAN PR")
//      println("ok")
    val res = svc.extrEnts(xtrMgr, mp)
    if (wholeStrMatch)
      res.filter(
        p => p._1.start == 0 && p._1.end == mp.input.linesOfTokens(0).length // should be the whole string
      )
    else res
  }

  private def loadDict(
                gnsvc: GNSvc,
                exVocabs:Iterable[Iterable[String]]
              ):Dict = {
    val words1 = MatcherGen.wordsFromGNSvc(gnsvc)
    val words2 = Vocabs.allWords
    InputHelpers.loadDict(words1++words2++exVocabs)
  }

  private val maxDiff = 0.5
  private[sparkRuns] def checkCoord(
                          lat1:Double,
                          long1:Double,
                          lat2:Double,
                          long2:Double
                        ):Boolean = {
    math.abs(lat1-lat2) < maxDiff && math.abs(long1-long2) < maxDiff
  }

  private[sparkRuns] def distByCoord(
                           lat1:Double,
                           long1:Double,
                           lat2:Double,
                           long2:Double
                         ):Double = {
    val latDiff = lat1-lat2
    val lonDiff = long1-long2
    latDiff*latDiff + lonDiff*lonDiff
  }

  private[sparkRuns] def csvRead(
    spSess: SparkSession,
    csvPath:String,
    meta: SrcCsvMeta
  ):DataFrame = {
    _csvRead(spSess, csvPath, meta.allCols)
  }

  private[sparkRuns] def _csvRead(
    spSess: SparkSession,
    csvPath:String,
    cols:Vector[String]
  ):DataFrame = {
    val (first, theRest) = cols.head -> cols.tail
    val rows = spSess.read
      .format("csv")
      .option("header", "true")
      .load(csvPath)
    rows.select(first, theRest: _*)
  }

  def replPfx(in:String, pfxMap:Map[String, String]):Option[String] = {
    val pfx2Repl = pfxMap.keySet.filter(in.startsWith)
    if (pfx2Repl.nonEmpty) {
      if (pfx2Repl.size > 1) throw new IllegalArgumentException("todo")
      val pfx = pfx2Repl.head
      val replaced = pfxMap(pfx) + in.substring(pfx.length)
      Option(replaced)
    } else None
  }

  def findNearestAndCheck(ents:Iterable[GNEnt], coord:(Double, Double)):Option[GNEnt] = {
    val nearest = ents
      .minBy(ent => distByCoord(ent.latitude, ent.longitude, coord._1, coord._2))
    if (!checkCoord(nearest.latitude, nearest.longitude, coord._1, coord._2)) {
      val nearestCoord = (nearest.latitude, nearest.longitude)
      val diff = f"(${coord._1-nearestCoord._1}%.2f,${coord._2-nearestCoord._2}%.2f)"
      println(s"Too far ($diff): ${nearest.gnid} $nearestCoord vs. $coord)")
      None
    }
    else {
      Option(nearest)
    }

  }

  def extrGNEnts(gnstr:String, gnm:GNMmgr, wholeStrMatch:Boolean, pfxReplMap:Map[String, String]):Map[TkRange, List[GNEnt]] =
    _extrGNEnts(gnstr, gnm, wholeStrMatch, Option(pfxReplMap))
  def extrGNEnts(gnstr:String, gnm:GNMmgr, wholeStrMatch:Boolean):Map[TkRange, List[GNEnt]] =
    _extrGNEnts(gnstr, gnm, wholeStrMatch, None)

  private def _extrGNEnts(
                           gnstr:String,
                           gnm:GNMmgr,
                           wholeStrMatch:Boolean,
                           pfxReplMap:Option[Map[String, String]]
                         ):Map[TkRange, List[GNEnt]] = {
    var rng2Ents = runStr(
      gnstr, gnm.tknr, gnm.dict, gnm.mmgr, gnm.svc, gnm.xtrMgr, wholeStrMatch
    )

    if (rng2Ents.isEmpty && pfxReplMap.nonEmpty) {
      val repl = replPfx(gnstr, pfxReplMap.get)
      if (repl.nonEmpty) {
        rng2Ents = runStr(
          repl.get,
          gnm.tknr, gnm.dict, gnm.mmgr, gnm.svc,
          gnm.xtrMgr, wholeStrMatch
        )
      }
    }
    rng2Ents
  }

}
