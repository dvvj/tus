package org.ditw.sparkRuns.pmXtr
import org.apache.spark.storage.StorageLevel
import org.ditw.common.GenUtils.printlnT0
import org.ditw.common.{ResourceHelpers, SparkUtils, TkRange}
import org.ditw.demo1.gndata.GNCntry.{PR, US}
import org.ditw.demo1.gndata.GNEnt
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.exutil1.naen.NaEnData.NaEnCat._
import org.ditw.exutil1.naen.TagHelper
import org.ditw.exutil1.naen.TagHelper.builtinTag
import org.ditw.matcher.MatchPool
import org.ditw.pmxml.model.AAAuAff
import org.ditw.sparkRuns.CommonUtils
import org.ditw.sparkRuns.CommonUtils.loadGNMmgr
import org.ditw.sparkRuns.csvXtr.{EntXtrUtils, IsniEnAlias}
import org.ditw.textSeg.common.Tags.TagGroup4Univ
import org.ditw.textSeg.output.{AffGN, SegGN}

import scala.collection.mutable.ListBuffer

object UtilsXtrCntry {


  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath =
      if (args.length > 1) args(1)
      else "file:///media/sf_vmshare/pmjs/pmj7AuAff/"  //"file:///media/sf_vmshare/pmjs/testAuAff/"  //
    val inputGNPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/gns/all"
    val outputPathJson = if (args.length > 3) args(3) else "file:///media/sf_vmshare/pmjs/9-x-json"
    val outputPathTrace = if (args.length > 4) args(4) else "file:///media/sf_vmshare/pmjs/9-x-agg"
    val parts = if (args.length > 5) args(5).toInt else 16

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

    val ccs = Set(US)
    val brCcs = spark.broadcast(ccs)


    val ufdEnts = EntXtrUtils.loadUfdNaEns("/media/sf_vmshare/ufd-isni.json")
//    val ufdAliases =

    import CommonUtils._
    import org.ditw.exutil1.naen.NaEnData._
    val gnmmgr = loadGNMmgr(
      ccs, Set(), spark,
      "file:///media/sf_vmshare/gns/all"
      ,Map(
//        builtinTag(US_UNIV.toString) -> UsUnivColls,
//        builtinTag(US_HOSP.toString) -> UsHosps,
        builtinTag(UFD.toString) -> ufdEnts
      )
    )
    val brGNMmgr = spark.broadcast(gnmmgr)


    printlnT0("Running extraction ...")

    import PmXtrUtils._
    val allSegs = segmentInput(spark, inputPath)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val singleSegs = allSegs.filter(_._3.length == 1)

    val multiSegs = allSegs.filter(_._3.length != 1)
    printlnT0(s"Single Segs: ${singleSegs.count()}, Multiple Segs: ${multiSegs.count()}")

    val (foundRes, emptyRes) = processSingleSegs(singleSegs, brGNMmgr, brCcs)

    printlnT0(s"Found: ${foundRes.count()}, Empty: ${emptyRes.count()} Saving results ...")
    val outJsonPath = "file:///media/sf_vmshare/pmjs/ssr"
    SparkUtils.del(spark, outJsonPath)

    foundRes
      .coalesce(parts)
      .sortBy(r => r.pmid -> r.localId)
      .map(singleRes2Json)
      .saveAsTextFile(outJsonPath)

    val outEmptyPath = "file:///media/sf_vmshare/pmjs/sse"
    SparkUtils.del(spark, outEmptyPath)
    emptyRes
      .coalesce(parts/4)
      .sortBy(s => s)
      .saveAsTextFile(outEmptyPath)

    spark.stop()
  }

}
