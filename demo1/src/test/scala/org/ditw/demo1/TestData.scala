package org.ditw.demo1
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{Dict, ResourceHelpers, SparkUtils}
import org.ditw.demo1.gndata.SrcData._
import org.ditw.demo1.gndata.{GNSvc, TGNMap}
import org.ditw.demo1.gndata.GNCntry._
import org.ditw.demo1.matchers.MatcherGen

object TestData {

  val testGNSvc:GNSvc = {
    val spark = SparkUtils.sparkContextLocal()

    val lines = ResourceHelpers.loadStrs("/test_gns.csv")

    val gnLines = spark.parallelize(lines).map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val cs = Set(
      US, CA, CN, JP, GB, PR
      //, "GB", "AU", "FR", "DE", "ES", "IT"
    )

    val svc = GNSvc.loadDef(gnLines, cs)

    svc
  }

  val testDict: Dict = MatcherGen.loadDict(testGNSvc)

}
