package org.ditw.sparkRuns.xmlXtr
import com.lucidchart.open.xtract.XmlReader
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.pmxml.model.{Arti, ArtiSet}

import scala.xml.XML

object ExtractPM {
  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath = if (args.length > 1) args(1) else "file:///media/sf_vmshare/pmjs/_xmls/*.xml"
    val outputPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/pmjs/_pmj"
    val parts = if (args.length > 3) args(3).toInt else 4

    val spark =
      if (runLocally) {
        SparkUtils.sparkContextLocal()
      }
      else {
        SparkUtils.sparkContext(false, "GeoRunMatchers", parts)
      }

    val parseResults = spark.wholeTextFiles(inputPath, parts)
      .map { p =>
        val xml = XML.loadString(p._2)
        val parsed = XmlReader.of[ArtiSet].read(xml)
        val artiSet:ArtiSet = parsed.getOrElse(ArtiSet.EmptyArtiSet)
        (p._1, artiSet, parsed.errors)
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val parseErrors:Map[String, Int] = parseResults
      .filter(_._3.nonEmpty)
      .map(p => p._1 -> p._3.size)
      .collect().toMap

    println(s"Error Map: $parseErrors")

    val parseRes = parseResults.flatMap(_._2.artis)
      .repartition(parts)
      .filter { arti =>
        val date = arti.citation.journal.getPubDate
        val res = date.year >= 2014
        if (!res) {
          println(s"${arti.citation.pmid} (published in ${date.year}) not included!")
        }
        res
      }
      .map(Arti.toJson)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    println("Saving ...")

    SparkUtils.del(spark, outputPath)
    println(s"$outputPath data deleted ...")
    parseRes.saveAsTextFile(outputPath)
    println("Done ...")

    spark.stop()

  }
}
