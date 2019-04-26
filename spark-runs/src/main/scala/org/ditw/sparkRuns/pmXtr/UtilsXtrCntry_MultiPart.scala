package org.ditw.sparkRuns.pmXtr
import org.apache.spark.storage.StorageLevel
import org.ditw.common.GenUtils.printlnT0
import org.ditw.common.{ResourceHelpers, SparkUtils}
import org.ditw.demo1.gndata.GNCntry._
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.exutil1.naen.NaEnData.NaEnCat.UFD
import org.ditw.exutil1.naen.TagHelper.builtinTag
import org.ditw.sparkRuns.CommonUtils
import org.ditw.sparkRuns.CommonUtils.loadGNMmgr
import org.ditw.sparkRuns.csvXtr.EntXtrUtils
import org.ditw.sparkRuns.entUtil.UniqGeoEnt

object UtilsXtrCntry_MultiPart {
  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath =
      if (args.length > 1) args(1)
      else "file:///media/sf_vmshare/pmjs/auAff2014p" //auAff2014p/auAff2014p_rem/auAff-dbg"  //"file:///media/sf_vmshare/pmjs/testAuAff/"  //
    val inputGNPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/gns/all"
    val outputPathJson = if (args.length > 3) args(3) else "file:///media/sf_vmshare/pmjs/9-x-json"
    val outputPathTrace = if (args.length > 4) args(4) else "file:///media/sf_vmshare/pmjs/9-x-agg"
    val parts = if (args.length > 5) args(5).toInt else 128

    val spark =
      if (runLocally) {
        SparkUtils.sparkContextLocal()
      }
      else {
        SparkUtils.sparkContext(false, "Full Extraction", parts)
      }

    val uniqGeoEnts = ResourceHelpers.load("/ufdent_uniq_geo.json", UniqGeoEnt.load)
    println(s"Unique Geo Entities: ${uniqGeoEnts.length}")

    val brUniqGeoMap = spark.broadcast(uniqGeoEnts.map{uge =>uge.id -> uge}.toMap)


    val gnLines = spark.textFile(
      //"/media/sf_vmshare/fp2Affs_uniq"
      inputGNPath
    ).map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val ccs = Set(US, CA)
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

    val rerun = true

    val allSegs = if (!rerun)
      affInputEx(spark, inputPath,
        "/media/sf_vmshare/pmjs/ssra-b1,/media/sf_vmshare/pmjs/ssra-b2"
      )
      else
        affInput(spark, inputPath)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    val singleSegs = allSegs.filter(_._3.length == 1)

//    val multiSegs = allSegs.filter(_._3.length != 1)
    printlnT0(s"Segs #: ${allSegs.count()}")

    val (foundAll, foundPart, emptyRes) = processAffStr(allSegs, brGNMmgr, brCcs, brUniqGeoMap)

    import ie.nod.sparkModels.AffResOut._

    printlnT0(s"Found: (${foundAll.count()}/${foundPart.count()}), Empty: ${emptyRes.count()} Saving results ...")
    val outJsonPathAll = "file:///media/sf_vmshare/pmjs/ssra"
    SparkUtils.del(spark, outJsonPathAll)
    foundAll
      .coalesce(parts)
      .sortBy(r => r.pmid -> r.localId)
      .map(affRes2Out)
      .map(affResOut2Json)
      .saveAsTextFile(outJsonPathAll)

//    val outJsonPathPart = "file:///media/sf_vmshare/pmjs/ssrp"
//    SparkUtils.del(spark, outJsonPathPart)
//    foundPart
//      .coalesce(parts)
//      .sortBy(r => r.pmid -> r.localId)
//      .map(affRes2Out)
//      .map(affResOut2Json)
//      .saveAsTextFile(outJsonPathPart)

    val outEmptyPath = "file:///media/sf_vmshare/pmjs/sse"
    SparkUtils.del(spark, outEmptyPath)
    emptyRes
      .coalesce(parts/4)
      .sortBy(s => s)
      .saveAsTextFile(outEmptyPath)

    spark.stop()
  }

}
