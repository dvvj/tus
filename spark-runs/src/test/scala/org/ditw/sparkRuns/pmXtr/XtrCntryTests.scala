package org.ditw.sparkRuns.pmXtr
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{ResourceHelpers, SparkUtils}
import org.ditw.sparkRuns.pmXtr.PmXtrUtils.{processSingleSegs, segmentInput}

object XtrCntryTests extends App {

  import org.ditw.sparkRuns.TestHelpers._
  val spark = SparkUtils.sparkContextLocal()
  val gnmmgr = testGNMmgr(spark)
  val brGNMmgr = spark.broadcast(gnmmgr)

  val allTests = ResourceHelpers.loadStrs(
    //"/xtr/dbg-part0"
    "/xtr/demo"
  )
  val allSegs = segmentInput(spark, allTests)
    .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val singleSegs = allSegs.filter(_._3.length == 1)

  val multiSegs = allSegs.filter(_._3.length != 1)

  val brCcs = spark.broadcast(_ccs)

  val (foundRes, emptyRes) = processSingleSegs(singleSegs, brGNMmgr, brCcs)


  println("-------------- Found")
  foundRes.foreach(println)

  println("-------------- Empty")
  emptyRes.foreach(println)
  spark.stop()
}
