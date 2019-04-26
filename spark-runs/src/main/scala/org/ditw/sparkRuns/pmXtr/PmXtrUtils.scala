package org.ditw.sparkRuns.pmXtr
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import ie.nod.sparkModels.{AffRes, AffResOut, SegRes}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.common.TkRange
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.{GNEnt, GNSvc}
import org.ditw.exutil1.naen.NaEnData.UsUnivColls
import org.ditw.exutil1.naen.TagHelper.NaEnId_Pfx
import org.ditw.exutil1.naen.{NaEn, NaEnData, TagHelper}
import org.ditw.matcher.{MatchPool, TkMatch}
import org.ditw.pmxml.model.AAAuAff
import org.ditw.sparkRuns.CommonUtils
import org.ditw.sparkRuns.CommonUtils.GNMmgr
import org.ditw.sparkRuns.entUtil.UniqGeoEnt
import org.ditw.textSeg.common.Tags.TagGroup4Univ
import org.json4s.DefaultFormats

import scala.collection.mutable.ListBuffer

object PmXtrUtils extends Serializable {
  private[pmXtr] def segment(affStr:String):Array[String] = {
    affStr.split(";").map(_.trim).filter(!_.isEmpty)
  }

  type SegmentRes = (Long, Int, Array[String])
  private[pmXtr] def segmentInput(spark: SparkContext, inputPath:String):RDD[SegmentRes] = {
    segmentInputRDD(spark.textFile(inputPath))
  }
  private[pmXtr] def segmentInputRDD(in:RDD[String]):RDD[SegmentRes] = {
    in
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val j = l.substring(firstComma + 1, l.length - 1)
        val auaff = AAAuAff.fromJson(j)
        val affMap = auaff.affs.map(aff => (pmid, aff.localId, segment(aff.aff.aff)))
        affMap
      }
  }

  private[pmXtr] def segmentInput(spark:SparkContext, in:Array[String]):RDD[SegmentRes] = {
    spark.parallelize(in)
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val j = l.substring(firstComma + 1, l.length - 1)
        val auaff = AAAuAff.fromJson(j)
        val affMap = auaff.affs.map(aff => (pmid, aff.localId, segment(aff.aff.aff)))
        affMap
      }
  }

  private[pmXtr] def affInput(spark:SparkContext, in:Array[String]):RDD[RawAffIn] = {
    affInputRDD(spark.parallelize(in))
  }
  private[pmXtr] def affInput(spark:SparkContext, path:String):RDD[RawAffIn] = {
    affInputRDD(spark.textFile(path))
  }

  private[pmXtr] def affInputEx(spark:SparkContext, path:String, processedPath:String):RDD[RawAffIn] = {
    val processed = loadProcessed(spark, processedPath)
        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    println(s"Processed #: ${processed.count()}")

    affInputRDDEx(spark.textFile(path), processed)
  }

  private val _EncBits = 36
  def encPmidLocalId(pmid:Long, localId:Int):Long = {
    (localId.toLong << _EncBits) + pmid
  }

  def decPmidLocalId(enc:Long):(Long, Int) = {
    val pmid = enc & 0xfffffffffL
    val localId = (enc >> _EncBits).toInt
    (pmid, localId)
  }

  private[pmXtr] def loadProcessed(spark:SparkContext, path:String):RDD[Long] = {
    spark.textFile(path).map(AffResOut.fromJson)
      .map { aro => encPmidLocalId(aro.pmid, aro.localId) }
  }
  private[pmXtr] def affInputRDD(in:RDD[String]):RDD[RawAffIn] = {
    in
      .flatMap { l =>
        var idxComma = l.indexOf(",")
        val pmid = l.substring(1, idxComma).toLong
        idxComma = l.indexOf(",", idxComma+1)
        val j = l.substring(idxComma + 1, l.length - 1)
        try {
          val auaff = AAAuAff.fromJson(j)
          val affMap = auaff.affs.map(aff => (pmid, aff.localId, aff.aff.aff))
          affMap
        }
        catch {
          case t:Throwable => {
            println(s"Error parsing $pmid: ${t.getMessage}")
            List((pmid, 0, ""))
          }
        }
      }
  }
  private[pmXtr] def affInputRDDEx(in:RDD[String], exclude:RDD[Long]):RDD[RawAffIn] = {
    val brEx = in.sparkContext.broadcast(exclude.collect().toSet)
    in
      .flatMap { l =>
        var idxComma = l.indexOf(",")
        val pmid = l.substring(1, idxComma).toLong
        idxComma = l.indexOf(",", idxComma+1)
        val j = l.substring(idxComma + 1, l.length - 1)
        try {
          val auaff = AAAuAff.fromJson(j)
          val affMap = auaff.affs
            .filter { aff =>
              val enc = encPmidLocalId(pmid, aff.localId)
              !brEx.value.contains(enc)
            }
            .map(aff => (pmid, aff.localId, aff.aff.aff))
          affMap
        }
        catch {
          case t:Throwable => {
            println(s"Error parsing $pmid: ${t.getMessage}")
            List((pmid, 0, ""))
          }
        }
      }
  }

  case class SingleSegRes(
    pmid:Long,
    localId:Int,
    seg:String,
    rangeStr:String,
    naen:NaEn
  ) {
    override def toString: String = {
      s"$pmid-$localId: [$seg] [$naen]"
    }
  }


  def singleRes2Json(ssr:SingleSegRes):String = {
    import org.json4s.jackson.Serialization._
    write(ssr)(DefaultFormats)
  }

  private def xtrNaEns(matchPool: MatchPool):Set[Long] = {
    val tags = matchPool.allTagsPrefixedBy(NaEnId_Pfx)
    val matches = matchPool.get(tags.toSet)
    val merged = TkMatch.mergeByRange(matches)
    merged.flatMap(_.getTags).filter(_.startsWith(NaEnId_Pfx))
      .map(_.substring(NaEnId_Pfx.length).toLong)
  }

  type AffRawRes = (String, Boolean, Option[AffRes])
  type RawAffIn = (Long, Int, String)

  import collection.mutable
  private[pmXtr] def processAffStr(
    affSegRes:RDD[RawAffIn],
    brGNMmgr:Broadcast[GNMmgr],
    brCcs:Broadcast[Set[GNCntry]],
    brUniqGeoEnts:Broadcast[Map[Long, UniqGeoEnt]]
  ):(RDD[AffRes], RDD[AffRes], RDD[String]) = {
    val ccsStr = brCcs.value.map(_.toString)
    val brCcsStr = affSegRes.sparkContext.broadcast(ccsStr)
    val t = affSegRes.map { tp3 =>
      val (pmid, localId, affStr) = tp3

      val affSegs = segment(affStr)
      val geosOfCntry = ListBuffer[GNEnt]()
      val segRes = affSegs.flatMap { aff =>
        val gnm = brGNMmgr.value
        var mp:MatchPool = null
        try {
          mp = MatchPool.fromStr(aff, gnm.tknr, gnm.dict)
        }
        catch {
          case t: Throwable => {
            println(s"error tokenizing $aff")
          }
        }
        gnm.mmgr.run(mp)
        val univRngs = mp.get(TagGroup4Univ.segTag).map(_.range)
        val rng2Ents = gnm.svc.extrEnts(gnm.xtrMgr, mp)
        val ents = rng2Ents.values.flatten
        val entsOfCntry = ents.filter(ent => brCcsStr.value.contains(ent.countryCode))
        var res:Option[SingleSegRes] = None
        //      if (pmid == 28288515L) // && localId == 2)
        //        println("ok")
        val neids = xtrNaEns(mp)
        val ugEnts = neids.filter(brUniqGeoEnts.value.contains)
          .flatMap(brGNMmgr.value.naEntDataMap.get)
        if (ugEnts.nonEmpty) {
          if (ugEnts.size > 1) {
            println(s"More than 1 uniq Geo entities: ${ugEnts.mkString(",")}, taking the first")
          }

          val segRes = SegRes(aff, "", ugEnts.head)
          Option(segRes)
        }
        else if (entsOfCntry.nonEmpty) {
          geosOfCntry ++= entsOfCntry
          val gnids = entsOfCntry.map(_.gnid).toSet
          val univs =
            if (univRngs.size == 1 && rng2Ents.size == 1) { // name fix, todo: better structure
              val univRng = univRngs.head
              val gnEntRng = rng2Ents.head._1
              if (univRng.overlap(gnEntRng) && gnEntRng.start > univRng.start) {
                val newRng = univRng.copy(end = gnEntRng.start)
                Set(newRng.str)
              }
              else {
                univRngs.map(_.str)
              }
            }
            else univRngs.map(_.str)

          import TagHelper._
          //        if (univs.size > 1)
          //          println(s"more than 1 seg found: $univs")
          val ents = neids.flatMap(brGNMmgr.value.naEntDataMap.get)
          val entsByGNid = ents.filter(e => checkGNids(e.gnid, gnids, brGNMmgr.value.svc))
          // todo: could be multiple entities with diff ids while pointing to the same entity
          val entsTr = entsByGNid.map { e =>
            val neid = e.neid
            val gnEnt = brGNMmgr.value.svc.entById(e.gnid).get
            s"$neid(${gnEnt.gnid}:${gnEnt.name})"
          }

          if (entsByGNid.nonEmpty) {
            val univStr = if (univs.nonEmpty) univs.head else ""
            val segRes = SegRes(aff, rng2Ents.keySet.head.str, entsByGNid.head)
            //println(s"Univs: [${univs.mkString(",")}] NEIds: [${entsTr.mkString(",")}]")
            Option(segRes)
          }
          else None
        }
        else None
        //(s"$pmid-$localId: $aff", entsOfCntry, res)

      }

      val md = MessageDigest.getInstance("MD5")
      val dig = md.digest(affStr.getBytes(StandardCharsets.UTF_8))
      val hs = dig.map(b => f"$b%x").mkString

      val geos = geosOfCntry.distinct.sortBy(_.gnid)
      val resAffStr = if (geosOfCntry.nonEmpty) Option(s"$affStr -- ${geos.mkString("|||")}") else None

      val affRes = AffRes(
        pmid, localId, hs, affSegs.length, segRes.toList, resAffStr
      )
      affRes
    }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val foundAll = t.filter(r => r.res.nonEmpty && r.segCount == r.res.size)
    val foundPart = t.filter(r => r.res.nonEmpty && r.segCount != r.res.size)
    val empty = t.filter(_.res.isEmpty)
      .flatMap { res =>
        res.affStr.map(affStr => s"${res.pmid}-${res.localId}: $affStr")
      }
    (foundAll, foundPart, empty)
  }


  type RawRes = (String, Boolean, Option[SingleSegRes])
  private[pmXtr] def processSingleSegs(
    singleSegs:RDD[SegmentRes],
    brGNMmgr:Broadcast[GNMmgr],
    brCcs:Broadcast[Set[GNCntry]]
  ):(RDD[SingleSegRes], RDD[String]) = {
    val ccsStr = brCcs.value.map(_.toString)
    val brCcsStr = singleSegs.sparkContext.broadcast(ccsStr)
    val t = singleSegs.map { tp3 =>
      val (pmid, localId, affSegs) = tp3

      val aff = affSegs(0) // single line
      val gnm = brGNMmgr.value
      var mp:MatchPool = null
      try {
        mp = MatchPool.fromStr(aff, gnm.tknr, gnm.dict)
      }
      catch {
        case t: Throwable => {
          println(s"error tokenizing $aff")
        }
      }
      gnm.mmgr.run(mp)
      val univRngs = mp.get(TagGroup4Univ.segTag).map(_.range)
      val rng2Ents = gnm.svc.extrEnts(gnm.xtrMgr, mp)
      val ents = rng2Ents.values.flatten
      val entsOfCntry = ents.filter(ent => brCcsStr.value.contains(ent.countryCode))
      var res:Option[SingleSegRes] = None
      //      if (pmid == 28288515L) // && localId == 2)
      //        println("ok")
      if (entsOfCntry.nonEmpty) {
        val gnids = entsOfCntry.map(_.gnid).toSet
        val univs =
          if (univRngs.size == 1 && rng2Ents.size == 1) { // name fix, todo: better structure
            val univRng = univRngs.head
            val gnEntRng = rng2Ents.head._1
            if (univRng.overlap(gnEntRng) && gnEntRng.start > univRng.start) {
              val newRng = univRng.copy(end = gnEntRng.start)
              Set(newRng.str)
            }
            else {
              univRngs.map(_.str)
            }
          }
          else univRngs.map(_.str)

        import TagHelper._
        //        if (univs.size > 1)
        //          println(s"more than 1 seg found: $univs")
        val neids = xtrNaEns(mp)
        val ents = neids.flatMap(brGNMmgr.value.naEntDataMap.get)
        val entsByGNid = ents.filter(e => checkGNids(e.gnid, gnids, brGNMmgr.value.svc))
        // todo: could be multiple entities with diff ids while pointing to the same entity
        val entsTr = entsByGNid.map { e =>
          val neid = e.neid
          val gnEnt = brGNMmgr.value.svc.entById(e.gnid).get
          s"$neid(${gnEnt.gnid}:${gnEnt.name})"
        }

        if (entsByGNid.nonEmpty) {
          val univStr = if (univs.nonEmpty) univs.head else ""
          val singleRes = SingleSegRes(pmid, localId, univStr, rng2Ents.keySet.head.str, entsByGNid.head)
          //println(s"Univs: [${univs.mkString(",")}] NEIds: [${entsTr.mkString(",")}]")
          res = Option(singleRes)
        }
        //else None
      }
      //else None
      (s"$pmid-$localId: $aff", entsOfCntry, res)


    }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val found = t.flatMap(_._3)
    val empty = t.filter(tp => tp._2.nonEmpty && tp._3.isEmpty)
      .map { tp =>
        val (affInfo, entsOfCntry, _) = tp
        s"$affInfo [$entsOfCntry]"
      }
    found -> empty
  }

  private[sparkRuns] def _checkGNidByDist(gnEnt1:GNEnt, gnEnt2:GNEnt):Boolean = {
    CommonUtils.checkCoord(
      gnEnt2.latitude,
      gnEnt2.longitude,
      gnEnt1.latitude,
      gnEnt1.longitude
    )
  }
  private[sparkRuns] def checkGNidByDist(gnEnt:GNEnt, gnid:Long, gnsvc: GNSvc):Boolean = {
    val ent = gnsvc.entById(gnid)
    if (ent.nonEmpty) {
      _checkGNidByDist(ent.get, gnEnt)
    }
    else false
  }

  private[sparkRuns] def checkGNids(naenGNid:Long, gnids:Set[Long], gnsvc: GNSvc):Boolean = {
    if (gnids.contains(naenGNid)) true
    else {
      val ent2Check = gnsvc.entById(naenGNid)
      if (ent2Check.isEmpty) {
        println(s"gnid: ${naenGNid} not in svc")
        false
      }
      else {
        gnids.exists { id => checkGNidByDist(ent2Check.get, id, gnsvc) }
      }
    }
  }
}
