package org.ditw.sparkRuns.pmXtr
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{ResourceHelpers, SparkUtils}
import org.ditw.sparkRuns.entUtil.UniqGeoEnt
import org.ditw.sparkRuns.pmXtr.PmXtrUtils.{affInput, processAffStr}

object XtrCntryTests_MultiSeg extends App {
  import org.ditw.sparkRuns.TestHelpers._
  val spark = SparkUtils.sparkContextLocal()
  val gnmmgr = testGNMmgr(spark)
  val brGNMmgr = spark.broadcast(gnmmgr)

  val allTests = ResourceHelpers.loadStrs(
    //"/xtr/dbg-part0"
    "/xtr/dbg-multi-seg"
  )
  val allAffs = affInput(spark, allTests)
    .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  //val singleSegs = allAffs.filter(_._3.length == 1)
  //val multiSegs = allSegs.filter(_._3.length != 1)

  val brCcs = spark.broadcast(_ccs)
  val uniqGeoEnts = ResourceHelpers.load("/ufdent_uniq_geo.json", UniqGeoEnt.load)
  println(s"Unique Geo Entities: ${uniqGeoEnts.length}")
  val brUniqGeoMap = spark.broadcast(uniqGeoEnts.map{uge =>uge.id -> uge}.toMap)

  val (foundAll, foundPart, emptyRes) = processAffStr(allAffs, brGNMmgr, brCcs, brUniqGeoMap)

  println("-------------- Found All")
  foundAll.foreach(println)

  println("-------------- Found Part")
  foundPart.foreach(println)

  println("-------------- Empty")
  emptyRes.foreach(println)
  spark.stop()
}
