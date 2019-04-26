package org.ditw.sparkRuns.es
import ie.nod.sparkModels.PmAffGeo
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
import org.ditw.sparkRuns.pmXtr.PmXtrUtils
import org.ditw.sparkRuns.pmXtr.PmXtrUtils.{affInput, affInputEx, processAffStr}

object GenAffGeo {
  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath =
      if (args.length > 1) args(1)
      else "file:///media/sf_vmshare/pmjs/ssra-ca-b1" //auAff2014p/auAff-dbg"  //"file:///media/sf_vmshare/pmjs/testAuAff/"  //
    val inputGNPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/gns/all"
    val outputPathJson = if (args.length > 3) args(3) else "file:///media/sf_vmshare/pmjs/esAffOut-b3"
    val parts = if (args.length > 5) args(5).toInt else 32

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

    val ccs = Set(US, CA)
    val brCcs = spark.broadcast(ccs)

    import CommonUtils._
    import org.ditw.exutil1.naen.NaEnData._
    val gnSvc = loadGNSvc(
      ccs,
      spark.textFile(
        "file:///media/sf_vmshare/gns/all"
      )
    )
    val brGNSvc = spark.broadcast(gnSvc)

    import ie.nod.sparkModels.AffResOut

    SparkUtils.del(spark, outputPathJson)
    spark.textFile(inputPath)
      .map(AffResOut.fromJson)
      .flatMap(affOut => PmAffGeo.affOut2PmAffGeo(affOut, brGNSvc.value))
      .coalesce(parts)
      .sortBy(_.pmid)
      .map(PmAffGeo.toJson)
      .saveAsTextFile(outputPathJson)

    spark.stop()
  }
}
