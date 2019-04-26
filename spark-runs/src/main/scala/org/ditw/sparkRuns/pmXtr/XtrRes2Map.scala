package org.ditw.sparkRuns.pmXtr
import ie.nod.sparkModels.AffResOut
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{GenUtils, SparkUtils}
import org.ditw.pmxml.model.AAAuAff
import org.ditw.sparkRuns.pmXtr.PmXtrUtils.RawAffIn

object XtrRes2Map {

  private def pmidCount(in:RDD[String]):Unit = {
    val multi = in
      .map { l =>
        var idxComma = l.indexOf(",")
        val pmid = l.substring(1, idxComma).toLong
        pmid -> 1
      }
      .reduceByKey(_ + _)
      .filter(_._2 > 1)
      .sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    multi.foreach(println)
  }

  import PmXtrUtils._

  def main(args:Array[String]):Unit = {
    val spark = SparkUtils.sparkContextLocal()

//    pmidCount(spark.textFile("/media/sf_vmshare/pmjs/auAff2014p"))

    val affResEncMap = spark.textFile("/media/sf_vmshare/pmjs/ssra-b1")
      .map { l =>
        val affRes = AffResOut.fromJson(l)
        val neids = affRes.res.map(_.eid)
        val enc = encPmidLocalId(affRes.pmid, affRes.localId)
        enc -> neids
      }.collectAsMap()

    println(s"Enc Map size: ${affResEncMap.size}")
    val brAffResEncMap = spark.broadcast(affResEncMap)

    val auAff = spark.textFile(
      //"/media/sf_vmshare/pmjs/auAff-dbg"
      "/media/sf_vmshare/pmjs/auAff2014p_rem"
    )
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val js = l.indexOf("{")
        val j = l.substring(js, l.length-1)
        val auAff = AAAuAff.fromJson(j)
        auAff.singleAuthors.indices.flatMap { idx =>
          val au = auAff.singleAuthors(idx)
          val allNeids = au.affLocalIds.flatMap { localId =>
            val enc = encPmidLocalId(pmid, localId)
            if (brAffResEncMap.value.contains(enc))
              brAffResEncMap.value(enc)
            else
              None
          }
          if (allNeids.nonEmpty)
            Option(s"$pmid,$idx,${allNeids.sorted.mkString(" ")}")
          else
            None
        }
      }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val outPath = "file:///media/sf_vmshare/pmjs/auXtrAff-b1"
    SparkUtils.del(spark, outPath)
    auAff.saveAsTextFile(outPath)

    spark.stop()
  }
}
