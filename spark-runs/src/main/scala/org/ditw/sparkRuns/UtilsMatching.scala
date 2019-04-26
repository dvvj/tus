package org.ditw.sparkRuns
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.demo1.gndata.{GNCntry, GNSvc}
import org.ditw.demo1.gndata.GNCntry._
import org.ditw.demo1.gndata.SrcData.{loadAdm0, loadCountries, tabSplitter}
import org.ditw.demo1.matchers.{Adm0Gen, MatcherGen, TagHelper}
import org.ditw.matcher.MatchPool
import org.ditw.tknr.TknrHelpers

object UtilsMatching extends App {
  val spark = SparkUtils.sparkContextLocal()

  val gnLines = spark.textFile(
    //"/media/sf_vmshare/fp2Affs_uniq"
    "/media/sf_vmshare/gns/all"
  ).map(tabSplitter.split)
    .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val ccs = Set(
    US, JP, PR
    // ,CA, GB, AU //,FR,DE,ES,IT
  )
  val svc = GNSvc.loadDef(gnLines, ccs)

  val dict = MatcherGen.loadDict(svc)

  val (mmgr, xtrMgr) = MatcherGen.gen(svc, dict, Set(PR))

  val brSvc = spark.broadcast(svc)
  val brMmgr = spark.broadcast(mmgr)
  val brXtrMgr = spark.broadcast(xtrMgr)
  val brDict = spark.broadcast(dict)
  val brTknr = spark.broadcast(TknrHelpers.TknrTextSeg())

  spark.textFile("/media/sf_vmshare/aff-w2v-dbg")
    .foreach { l =>
      val mp = MatchPool.fromStr(l, brTknr.value, brDict.value)
      brMmgr.value.run(mp)
      val t = TagHelper.cityCountryTag(GNCntry.US)
      val rng2Ents = brSvc.value.extrEnts(brXtrMgr.value, mp)
      println(rng2Ents)
    }

  spark.stop()
}
