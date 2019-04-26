package org.ditw.sparkRuns
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.demo1.src.SrcDataUtils
import org.ditw.sparkRuns.SegMatcherRuns.spark

object UtilsSlimGNs extends Serializable {

  import org.ditw.demo1.src.SrcDataUtils._
  import org.ditw.demo1.src.SrcDataUtils.GNsCols

  private val tabSplitter = "\\t".r
  private val commaSplitter = ",".r

  private def runTask(
    rdd:RDD[IndexedSeq[String]],
    featureChecker: FeatureChecker,
    outPath:String):Unit= {

    val featureClassIndex = 5
    val featureCodeIndex = 6
    val res = rdd.filter(l => featureChecker(l(featureClassIndex), l(featureCodeIndex)))
      .map(_.mkString("\t"))
      .cache()
    println(s"Result #: ${res.count()}")

    SparkUtils.deleteLocal(outPath)
    res.saveAsTextFile(outPath)
  }

  def main(args:Array[String]):Unit = {
    val spark = SparkUtils.sparkContextLocal()
    val gnLines = spark.textFile(
      //"/media/sf_vmshare/fp2Affs_uniq"
      "/media/sf_vmshare/gns/allCountries.txt"
    )

    val slimmed = gnLines.map { l =>
      val line = tabSplitter.split(l)

      val altNameCount = commaSplitter.split(
        GNsCol(line, GNsCols.AltNames)
      ).filter(_.nonEmpty).size

      val newLineCols = SrcDataUtils.GNsSlimColArr

      val newLine = newLineCols.map(GNsCol(line, _))

//      if (line(0) == "4956184")
//        println("ok")

      val res = newLine :+ altNameCount.toString
        //(newLine :+ altNameCount).mkString("\t")
      res
    }.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    runTask(
      slimmed,
      SrcDataUtils.fcAdm,
      "/media/sf_vmshare/gns/adms"
    )
    runTask(
      slimmed,
      SrcDataUtils.fcPpl,
      "/media/sf_vmshare/gns/ppls"
    )
    runTask(
      slimmed,
      SrcDataUtils.fcAll,
      "/media/sf_vmshare/gns/all"
    )

    spark.stop()
  }

}
