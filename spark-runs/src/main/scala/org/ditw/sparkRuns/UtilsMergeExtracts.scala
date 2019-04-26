package org.ditw.sparkRuns
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.textSeg.output.{AffGN, SegGN}

object UtilsMergeExtracts {

  private def checkStats(msg:String, segGN:RDD[(String, SegGN)]):Unit = {
    val segCount = segGN.count
    val segGnCount = segGN.values.map(_.affGns.size).sum().toInt
    val affFpCount = segGN.values.flatMap(_.affGns.map(_.pmAffFps.length)).sum().toInt
    println(s"$msg: $segCount/$segGnCount/$affFpCount")
  }

  def mergeAffGN(agn1: AffGN, agn2: AffGN):AffGN = {
    assert(agn1.gnid == agn2.gnid)
    agn1.copy(pmAffFps = (agn1.pmAffFps++agn2.pmAffFps).sorted.distinct)
  }

  def mergeSegGN(sgn1:SegGN, sgn2:SegGN):SegGN = {
    assert(sgn1.name == sgn2.name)
    val gnid1s = sgn1.affGns.map(_.gnid).toSet
    val gnid2s = sgn2.affGns.map(_.gnid).toSet
    val shared = gnid1s.intersect(gnid2s)
    val sgn1Only = (gnid1s -- shared).map(sgn1.affGn)
    val sgn2Only = (gnid2s -- shared).map(sgn2.affGn)
    val mergedShared = shared.map(id => mergeAffGN(sgn1.affGn(id), sgn2.affGn(id)))
    val allAffGNs = (sgn1Only ++ sgn2Only ++ mergedShared).toVector.sortBy(_.gnid)
    sgn1.copy(
      affGns = allAffGNs
    )
  }

  def doMerge(existing:RDD[(String, SegGN)], toAdd:RDD[(String, SegGN)]):RDD[SegGN] = {
    val existingLOJ = existing.leftOuterJoin(toAdd)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val merged = existingLOJ.filter(_._2._2.nonEmpty).mapValues { p =>
      val ex = p._1
      val n = p._2.get
      mergeSegGN(ex, n)
    }
    val existingOnly = existingLOJ.filter(_._2._2.isEmpty).map(_._2._1)
    val toAddOnly = toAdd.leftOuterJoin(existing).filter(_._2._2.isEmpty).map(_._2._1)
    merged.values.union(existingOnly).union(toAddOnly)
  }

  private def loadSgns(spark:SparkContext, path:String):RDD[(String, SegGN)] = {
    spark.textFile(path)
      .map { j =>
        val sgn = SegGN.fromJson(j)
        sgn.name -> sgn
      }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  }

//  private def randSamples(sgns:RDD[(String, SegGN)]):Unit = {
//
//  }

  def main(args:Array[String]):Unit = {
    val spark = SparkUtils.sparkContextLocal()
    val existing = loadSgns(spark, "/media/sf_vmshare/pmjs/curated")
    checkStats("Existing", existing)

    val toAdd = loadSgns(spark, "/media/sf_vmshare/pmjs/mergeTmp")
    checkStats("ToAdd", toAdd)

    val merged = doMerge(existing, toAdd).map(sgn => sgn.name -> sgn)
      .sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    checkStats("Merged", merged)

    val path = "file:///media/sf_vmshare/pmjs/merged"
    SparkUtils.del(spark, path)
    merged.values.map(SegGN.toJson)
      .saveAsTextFile(path)

    spark.stop()
  }
}
