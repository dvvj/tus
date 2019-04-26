package org.ditw.demo1.gndata
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.common.GenUtils.printlnT0
import org.ditw.common.ResourceHelpers
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.GNLevel.{ADM4, GNLevel}
import org.ditw.demo1.gndata.GNSvc.LoadSettings
import org.ditw.demo1.src.SrcDataUtils
import org.ditw.demo1.src.SrcDataUtils.GNsCols
import org.ditw.demo1.src.SrcDataUtils.GNsCols._

import scala.collection.mutable.ListBuffer

object SrcData extends Serializable {
  val tabSplitter = "\\t".r


  val gnidIndex = 0
  val featureCodeIndex = 6
  val countryCodeIndex = 7
  val populationIndex = SrcDataUtils.GNsSlimColArr.length - 1
  private val colEnum2Idx = SrcDataUtils.GNsSlimColArrAltCount.indices.map { idx =>
    SrcDataUtils.GNsSlimColArrAltCount(idx) -> idx
  }.toMap
  def colVal(cols:Array[String], col: GNsCols):String = {
    cols(colEnum2Idx(col))
  }
  private val EmptyMap = Map[Long, GNEnt]()
  private val EmptySubAdms = IndexedSeq[String]()
  private val EmptyAdms = IndexedSeq[String]()

  def entFrom(cols:Array[String], countryCode:String, admCodes:IndexedSeq[String]):GNEnt = {
    GNEnt(
      colVal(cols, GNsCols.GID).toLong,
      colVal(cols, GNsCols.Name),
      Set(colVal(cols, GNsCols.AsciiName)), // todo
      colVal(cols, GNsCols.Latitude).toDouble,
      colVal(cols, GNsCols.Longitude).toDouble,
      colVal(cols, GNsCols.FeatureClass),
      colVal(cols, GNsCols.FeatureCode),
      countryCode,
      admCodes,
      colVal(cols, GNsCols.Population).toLong
    )
  }

  def loadAdm0(rdd:RDD[Array[String]]):Map[String, GNEnt] = {
    val adm0Ents = rdd.filter(cols => SrcDataUtils.isAdm0(cols(featureCodeIndex), cols(countryCodeIndex)))
      .map { cols =>
        val countryCode = colVal(cols, GNsCols.CountryCode)
        val ent = entFrom(cols, countryCode, EmptyAdms)
        countryCode -> ent
      }.collectAsMap()

    adm0Ents.toMap
  }


  val admCols = List(GNsCols.Adm1, GNsCols.Adm2, GNsCols.Adm3, GNsCols.Adm4)
  private val admIndexMap = Map(
    Adm1 -> 9,
    Adm2 -> 10,
    Adm3 -> 11,
    Adm4 -> 12
  )
  private val indexAdmMap = admIndexMap.map(p => p._2 -> p._1)
  import org.ditw.demo1.gndata.GNLevel._
  private val emptyCol2Level:Map[GNsCols, GNLevel] = Map(
    Adm1 -> ADM0,
    Adm2 -> ADM1,
    Adm3 -> ADM2,
    Adm4 -> ADM3
  )

  private def admCode(gncols:Array[String]):(String, GNLevel) = {

    if (gncols(featureCodeIndex) == "PCLI")
      println("error")
    var res = gncols(countryCodeIndex)
    var empty = false
    var level:GNLevel = ADM4
    val colIdxSorted = admIndexMap.values.toIndexedSeq.sorted
    val it = colIdxSorted.iterator
    while (it.hasNext && !empty) {
      val idx = it.next()
      if (gncols(idx).nonEmpty) {
        if (empty)
          throw new IllegalArgumentException("Already empty?!")
        res += s"_${gncols(idx)}"
      }
      else {
        empty = true
        level = emptyCol2Level(indexAdmMap(idx))
      }
    }
    //    if (level.isEmpty)
    //      println("ok")
    res -> level
  }

  def admMinus1Code(admCode:String):Option[String] = {
    val lastIdx = admCode.lastIndexOf("_")
    if (lastIdx > 0) {
      Option(admCode.substring(0, lastIdx))
    }
    else None
  }

  private val aliases:Array[SrcAlias] = ResourceHelpers.load("/alias.json", SrcAlias.load)
  private val popu0s:Array[SrcPopu0] = ResourceHelpers.load("/popu0_whitelist.json", SrcPopu0.load)
  private val popu0Map = popu0s.map(p => p.gnid -> p.name).toMap

  private val _aliasMap:Map[Long, Iterable[String]] = aliases.map(a => a.gnid -> a.aliases).toMap

  def loadCountries(
    rdd:RDD[Array[String]],
    countries:Set[GNCntry],
    brAdm0Ents:Broadcast[Map[String, GNEnt]],
    settings: LoadSettings,
    aliasMap:Map[Long, Iterable[String]] = _aliasMap
  ):Map[GNCntry, TGNMap] = {

    val countryCodes = countries.map(_.toString)
    val gnsInCC:RDD[((String,GNLevel), GNEnt)] = rdd
      .filter { cols =>
        val popu = cols(populationIndex).toLong
        val gnid = cols(gnidIndex).toLong
//        if (gnid == 4920423L)
//          println("ok")
        if (countryCodes.contains(cols(countryCodeIndex))) {
          if (popu >= settings.minPopu) {
              !SrcDataUtils.isAdm0(cols(featureCodeIndex), cols(countryCodeIndex))
          }
          else {
            popu0Map.contains(gnid)
          }
        }
        else
          false
      }
      .map { cols =>
        val adms = ListBuffer[String]()
        admCols.foreach { col =>
          if (colVal(cols, col).nonEmpty) {
            adms += colVal(cols, col)
          }
        }
        val countryCode = colVal(cols, GNsCols.CountryCode)
        val ent = entFrom(
          cols,
          countryCode,
          adms.toIndexedSeq
        )

        val admc = admCode(cols)
        admc -> ent
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    printlnT0(s"\tADM->Ent: ${gnsInCC.count}")

    val t1:RDD[(Option[String], Iterable[(GNLevel, String)])] = gnsInCC
      .groupByKey()
      .map { p =>
        var admEnt:Option[GNEnt] = None
        val admCode = p._1._2.toString
        val ents = p._2
        ents.foreach { ent =>
          if (ent.featureCode == admCode) {
            if (admEnt.nonEmpty)
              throw new IllegalArgumentException("dup adm entity?")
            admEnt = Option(ent)
          }
        }

        val admM1 = admMinus1Code(p._1._1)
        val level =
          if (admEnt.nonEmpty) admEnt.get.level
          else p._1._2
        admM1 -> (level, p._1._1)
      }
      .groupByKey()
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val admGNs = gnsInCC.groupByKey()
      .map(p => p._1._1 -> (p._1._2 -> p._2))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val withAdmc = t1.filter { p =>
      //        if (p._1.isEmpty)
      //          println("ok")
      p._1.nonEmpty
    }.map(p => p._1.get -> p._2)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)


    val t3 = admGNs
      .filter { p =>
        !countryCodes.contains(p._1) || p._2._1 != ADM0
      }
      .leftOuterJoin(withAdmc)
      //.filter(_._2._2.isEmpty)
      .map { p =>
        val admc = p._1
        val (level, curr) = p._2._1
        val (currAdm, ppls) = splitAdmAndPpl(curr, level)
        val subAdms =
          if (p._2._2.isEmpty) EmptySubAdms
          else p._2._2.get.map(_._2).toIndexedSeq

        val admEnt =
          if (level == ADM0 && currAdm.isEmpty) {
            brAdm0Ents.value.get(admc)
          }
          else if (level == ADM0 && currAdm.nonEmpty)
            currAdm
          else
            currAdm
        val coll = GNColls.admx(
          level,
          admEnt,
          subAdms,
          ppls.map(ppl => ppl.gnid -> ppl).toMap
        )
        //        if (level == ADM0)
        //          println("ok")
        val cc = ccFromAdmc(admc)
        cc -> (admc -> coll)
      }
      .groupByKey()
      .mapValues(_.toMap)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val t3Keys = t3.map(_._1).collect().toSet

    val adm0s = withAdmc.filter(p => countryCodes.contains(p._1))
    val adm0Keys = adm0s.map(_._1).collect().toSet

    if (t3Keys != adm0Keys)
      throw new RuntimeException(s"Key set not match: $t3Keys <<-->> $adm0Keys")
    //val m2 = t3.collectAsMap().toMap

    //assert (!m2.contains(cc))
    val adm0GNs = admGNs.filter { p =>
      countryCodes.contains(p._1) && p._2._1 == ADM0
    }.collect().toMap
    val brAdm0GNs = rdd.sparkContext.broadcast(adm0GNs)

    val t = t3.join(adm0s)
      .map { p =>
        val (cc, pp) = p
        val gentOfAdm0 =
          if (!brAdm0GNs.value.contains(cc)) EmptyMap
          else {
            brAdm0GNs.value(cc)._2.map(ent => ent.gnid -> ent).toMap
          }
        println(s"\t$cc: ${gentOfAdm0.size}")
        val admEnt = brAdm0Ents.value(cc)
        val admMap = pp._1
        GNCollPreConstruct.preprocess(GNCntry.withName(cc), admMap)
        val adm0 = GNColls.adm0(
          admEnt, // todo
          pp._2.map(_._2).toIndexedSeq,
          gentOfAdm0,
          admMap,
          aliasMap
        )
        // printlnT0(adm0)
        adm0
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val res = t.collect()

    val totalEnts = res.map(_.size).sum

    printlnT0(s"Done: ${res.length} collected ($totalEnts entities)")

    // diff
    val allIds = gnsInCC.map(_._2.gnid -> 1)
    val missing = allIds.leftOuterJoin(
      t.flatMap(adm0 => adm0.admIdMap.flatMap(_._2.map(_._1 -> 1)))
    ).filter(_._2._2.isEmpty).map(_._1).collect()
    printlnT0(s"Missing: ${missing.length}")

    res.map(adm0 => adm0.countryCode -> adm0).toMap

  }

  private def ccFromAdmc(admc:String):String = {
    val firstUnderscore = admc.indexOf("_")
    admc.substring(0, firstUnderscore)
  }

  private def splitAdmAndPpl(in:Iterable[GNEnt], level:GNLevel)
  :(Option[GNEnt], Iterable[GNEnt]) = {
    var currAdm:Option[GNEnt] = None
    val ppls = in.filter { c =>
      if (c.featureCode == level.toString) {
        currAdm = Option(c)
        false
      }
      else true
    }
    currAdm -> ppls
  }
}
