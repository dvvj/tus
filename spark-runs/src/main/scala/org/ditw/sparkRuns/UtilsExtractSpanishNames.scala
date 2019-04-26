package org.ditw.sparkRuns
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{ResourceHelpers, SparkUtils}
import org.ditw.pmxml.model.AAAuAff

object UtilsExtractSpanishNames {
  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath =
      if (args.length > 1) args(1) else "file:///media/sf_vmshare/pmjs/pmj8AuAff/"
    val outputPath =
      if (args.length > 2) args(2)
      else "file:///media/sf_vmshare/pmjs/sp_names"
    val parts = if (args.length > 3) args(3).toInt else 4

    val spark =
      if (runLocally) {
        SparkUtils.sparkContextLocal()
      }
      else {
        SparkUtils.sparkContext(false, "Extract Spanish Names", parts)
      }

    val splitter = "\\s+".r

    val spNameSet = ResourceHelpers.loadStrs("/sp_lnames.txt")
      .map(_.toLowerCase).toSet
    val brSpNameSet = spark.broadcast(spNameSet)

    import org.ditw.common.GenUtils._
    import collection.mutable
    val spNames = spark.textFile(inputPath)
      .flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        val j = l.substring(firstComma+1, l.length-1)
        val auaff = AAAuAff.fromJson(j)
        auaff.singleAuthors.flatMap { au =>
          val normedForeName = normalize(au.foreName)
          val normedLastName = normalize(au.foreName)
          val foreNameParts = splitter.split(normedForeName).map(_.toLowerCase())
          val lastNameParts = splitter.split(normedLastName).map(_.toLowerCase())
          val foreNameSpParts = foreNameParts.filter(brSpNameSet.value.contains)
          val lastNameSpParts = lastNameParts.filter(brSpNameSet.value.contains)
          val spNameParts = foreNameSpParts ++ lastNameSpParts
          val origName = normalize(s"${au.foreName} ${au.lastName}").toLowerCase()

          if (spNameParts.nonEmpty) {
            Option(s"${spNameParts.sorted.head}|$origName" -> pmid)
          }
          else {
            None
          }
        }
      }
      .aggregateByKey(mutable.Set[Long]())(
        (s, i) => s + i,
        (s1, s2) => s1 ++ s2
      )
      .mapValues(_.toVector.sorted)
      .coalesce(8)
      .sortBy(_._1)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    SparkUtils.saveDelExisting(spNames, outputPath)

    spark.stop()
  }
}
