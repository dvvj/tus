package org.ditw.sparkRuns
import org.ditw.common.SparkUtils
import org.ditw.pmxml.model.AAAuAff

object UtilsExtractLongNames {
  import collection.mutable
  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath = if (args.length > 1) args(1) else "file:///media/sf_vmshare/pmj9AuAff/"
    val outputPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/pmjs/longnames"
    val parts = if (args.length > 3) args(3).toInt else 4

    val spark =
      if (runLocally) {
        SparkUtils.sparkContextLocal()
      }
      else {
        SparkUtils.sparkContext(false, "Extract Long Names", parts)
      }

    val splitter = "\\s+".r
    val pmidSet = mutable.Set[Long]()
    val sortedLongNames = spark.textFile(inputPath)
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val j = l.substring(firstComma+1, l.length-1)
        val auaff = AAAuAff.fromJson(j)
        auaff.singleAuthors.filter { au =>
          val foreNameParts = splitter.split(au.foreName)
          val lastNameParts = splitter.split(au.lastName)
          foreNameParts.length + lastNameParts.length > 4
        }.map { au =>
          s"${au.foreName}|${au.lastName}" -> pmid
        }
      }
      .aggregateByKey(pmidSet)(
        (s, id) => s += id,
        (s1, s2) => s1 ++= s2
      )
      .mapValues(_.toVector.sorted)
      .sortBy(_._1)

    SparkUtils.del(spark, outputPath)
    sortedLongNames.saveAsTextFile(outputPath)

    spark.stop()
  }
}
