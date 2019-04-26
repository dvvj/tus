package org.ditw.sparkRuns
import org.apache.spark.storage.StorageLevel
import org.ditw.common.GenUtils.printlnT0
import org.ditw.common.{SparkUtils, TkRange}
import org.ditw.demo1.gndata.GNCntry.{JP, PR, US}
import org.ditw.demo1.gndata.{GNEnt, GNSvc}
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.exutil1.naen.NaEnData.NaEnCat._
import org.ditw.exutil1.naen.TagHelper
import org.ditw.matcher.MatchPool
import org.ditw.pmxml.model.AAAuAff
import org.ditw.sparkRuns.csvXtr.EntXtrUtils
import org.ditw.textSeg.common.Tags.TagGroup4Univ
import org.ditw.textSeg.output.{AffGN, SegGN}

import scala.collection.mutable.ListBuffer

object UtilsExtrFull2 {

  def segment(affStr:String):Array[String] = {
    affStr.split(";").map(_.trim).filter(!_.isEmpty)
  }

  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath =
      if (args.length > 1) args(1)
      else "file:///media/sf_vmshare/pmjs/pmj9AuAff/"  //"file:///media/sf_vmshare/pmjs/testAuAff/"  //
    val inputGNPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/gns/all"
    val outputPathJson = if (args.length > 3) args(3) else "file:///media/sf_vmshare/pmjs/9-x-json"
    val outputPathTrace = if (args.length > 4) args(4) else "file:///media/sf_vmshare/pmjs/9-x-agg"
    val parts = if (args.length > 5) args(5).toInt else 4

    val spark =
      if (runLocally) {
        SparkUtils.sparkContextLocal()
      }
      else {
        SparkUtils.sparkContext(false, "Full Extraction", parts)
      }


    val gnLines = spark.textFile(
      //"/media/sf_vmshare/fp2Affs_uniq"
      inputGNPath
    ).map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val ccs = Set(US, PR)

    import CommonUtils._
    val gnmmgr = loadGNMmgr(
      ccs, Set(PR), spark,
      "file:///media/sf_vmshare/gns/all"
      ,Map(
        TagHelper.builtinTag(UFD.toString) -> EntXtrUtils.loadIsniNaEns("/media/sf_vmshare/ufd-isni.json")
      )
    )
    val brGNMmgr = spark.broadcast(gnmmgr)
    //    val (mmgr, xtrMgr) = genMMgr(svc, dict)
    //    val brSvc = spark.broadcast(svc)
    //    val brMmgr = spark.broadcast(mmgr)
    //    val brXtrMgr = spark.broadcast(xtrMgr)
    //    val brDict = spark.broadcast(dict)
    //    val brTknr = spark.broadcast(TknrHelpers.TknrTextSeg())

    printlnT0("Running extraction ...")

    val allSegs = spark.textFile(inputPath)
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val j = l.substring(firstComma + 1, l.length - 1)
        val auaff = AAAuAff.fromJson(j)
        val affMap = auaff.affs.map(aff => (pmid, aff.localId, segment(aff.aff.aff)))
        affMap
      }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val singleSegs = allSegs.filter(_._3.length == 1)

    val multiSegs = allSegs.filter(_._3.length != 1)

    val xtrs = singleSegs
      .map { tp3 =>
        val (pmid, localId, affSegs) = tp3
        val aff = affSegs(0) // single line
        val gnm = brGNMmgr.value
        val mp = MatchPool.fromStr(aff, gnm.tknr, gnm.dict)
        gnm.mmgr.run(mp)
        val univRngs = mp.get(TagGroup4Univ.segTag).map(_.range)
        val rng2Ents = gnm.svc.extrEnts(gnm.xtrMgr, mp)
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
        if (univs.nonEmpty) {
          val neids = mp.allTagsPrefixedBy(TagHelper.NaEnId_Pfx)
          println(s"$aff:\n\tNEIds: ${neids.mkString(",")}")
        }
        (pmid, localId, (aff, rng2Ents.map(identity), univs))

      }.sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    printlnT0("Saving results ...")

//    if (runLocally) {
//      val savePathEmpty = "/media/sf_vmshare/pmjs/9-e"
//      SparkUtils.deleteLocal(savePathEmpty)
//      xtrs.filter(xtr => xtr._3._2.isEmpty || xtr._3._3.isEmpty).saveAsTextFile(savePathEmpty)
//    }

    val hasXtrs1 = xtrs.filter(xtr => xtr._3._2.size == 1 && xtr._3._3.size == 1)
//    if (runLocally) {
//      val savePath1 = "/media/sf_vmshare/pmjs/9-x-s"
//      SparkUtils.deleteLocal(savePath1)
//      hasXtrs1
//        .map { p =>
//          val (pmid, localId, pp) = p
//          trace(pmid, localId, pp)
//        }
//        .saveAsTextFile(savePath1)
//    }

    SparkUtils.del(spark, outputPathJson)
    val segGns = hasXtrs1.map { p =>
      val (pmid, localId, pp) = p
      val univ = p._3._3.head
      val gnEnt = p._3._2.values.head.head
      //        if (univ.endsWith(gnEnt.name))
      //          univ = univ.substring(0, univ.length-gnEnt.name.length).trim // name fix, todo: better structure
      (
        univ,
        (gnEnt.gnid, gnEnt.toString, pmid, localId)
      )
    }
      .groupBy(_._1.toLowerCase())
      .map { pp =>
        val p = pp._2
        val name2Count = p.map(_._1 -> 1).groupBy(_._1).mapValues(_.map(_._2).sum)
        val nameRes = name2Count.maxBy(_._2)._1
        val affGns = p.map(_._2).groupBy(_._1)
          .mapValues { pp =>
            val gnid = pp.head._1
            val gnstr = pp.head._2
            val affFps = pp.map { tp => s"${tp._3}-${tp._4}" }
            (gnid, gnstr, affFps)
          }.map(identity)
        nameRes -> affGns
        //        p.map(_._2).groupBy(_._1)
        //          .mapValues { pp =>
        //            val gnstr = pp.head._2
        //            val fps = pp.toList.sortBy(_._3).map(tp => s"${tp._3}-${tp._4}")
        //            s"$gnstr #${fps.size}: ${fps.mkString(",")}"
        //          }.toList.sortBy(_._2)
        //          .map(_._2)
        //          .mkString("\t", "\n\t", "")
      }
      .flatMap { p =>
        val affGns = p._2.values
          .filter(_._3.size >= 5)
          .map { tp =>
            AffGN(tp._1, tp._2, tp._3.toSeq.sorted)
          }
        if (affGns.nonEmpty) {
          val segRes = SegGN(
            p._1, affGns.toSeq
          )
          Option(p._1 -> segRes)
        }
        else None
      }
      .sortBy(_._1.toLowerCase())
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    segGns.mapValues(SegGN.toJson)
      .values
      .saveAsTextFile(outputPathJson)

    SparkUtils.del(spark, outputPathTrace)
    segGns.sortBy(_._1.toLowerCase())
      .map { p =>
        val segGn = p._2
        val affTr = segGn.affGns.map { affGn =>
          val fps = affGn.pmAffFps.sorted.mkString(",")
          s"${affGn.gnrep}: $fps"
        }.mkString("\t", "\n\t", "")
        s"${segGn.name}\n$affTr"
      }
      .saveAsTextFile(outputPathTrace)

//    if (runLocally) {
//      val hasXtrs = xtrs.filter(xtr => xtr._3._2.size > 1 && xtr._3._3.nonEmpty || xtr._3._2.nonEmpty && xtr._3._3.size > 1)
//      val savePath = "/media/sf_vmshare/pmjs/9-x-m"
//      SparkUtils.deleteLocal(savePath)
//      hasXtrs.map { p =>
//        val (pmid, localId, pp) = p
//        trace(pmid, localId, pp)
//      }.saveAsTextFile(savePath)
//    }


    spark.stop()
  }

  def trace(pmid:Long, localId:Int,
            pp:(String, Map[TkRange, List[GNEnt]], Set[String])):String = {
    val trs = ListBuffer[String]()
    pp._2.keySet.toList.sorted.map { range =>
      val ents = pp._2(range)
      val trsEnts = ents.mkString("[", "],[", "]")
      trs += s"$range: $trsEnts"
    }
    val univStrs = pp._3.toList.sorted.mkString("---{", "},{", "}")
    trs += univStrs
    val trStr = trs.mkString("; ")
    s"$pmid-$localId: [${pp._1}]\n\t$trStr"
  }
}
