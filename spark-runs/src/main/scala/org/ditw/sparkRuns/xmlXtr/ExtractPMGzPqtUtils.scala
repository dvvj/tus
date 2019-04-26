package org.ditw.sparkRuns.xmlXtr
import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets

import com.lucidchart.open.xtract.XmlReader
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.pmxml.model.ArtiSet

import scala.xml.XML

object ExtractPMGzPqtUtils {
  case class ArtiStats(name:String, total:Int, filteredCount:Int) {
    override def toString: String = {
      s"$name: $filteredCount/$total"
    }
  }

  private def parseBySpark(inputPath:String, startYear:Int, runLocally:Boolean, parts:Int):Unit = {
    val spark =
      if (runLocally) {
        SparkUtils.sparkSessionLocal()
      }
      else {
        SparkUtils.sparkSession(false, "GeoRunMatchers", parts)
      }

    val parseResults = spark.sparkContext.binaryFiles(inputPath, parts)
      .map { p =>
        val (fn, strm) = p

        val gzStrm = new GzipCompressorInputStream(strm.open())
        //gzStrm.getNextEntry
        val xmlStr = IOUtils.toString(gzStrm, StandardCharsets.UTF_8)
        gzStrm.close()
        val xml = XML.loadString(xmlStr)
        val parsed = XmlReader.of[ArtiSet].read(xml)
        val artiSet:ArtiSet = parsed.getOrElse(ArtiSet.EmptyArtiSet)
        val allArtiCount = artiSet.artis.size
        val artisFiltered = artiSet.artis.filter(_.citation.journal.getPubDate.year >= startYear)
        val artiSetFiltered = artiSet.copy(artis = artisFiltered)
        val stats = ArtiStats(fn, allArtiCount, artisFiltered.size)
        println(stats)
        (p._1, artiSetFiltered.artis.size, parsed.errors, stats)
      }
    val parseErrors:Map[String, Int] = parseResults
      .filter(_._3.nonEmpty)
      .map(p => p._1 -> p._3.size)
      .collect().toMap

    println(s"Error Map: $parseErrors")

    spark.stop()
  }

  def parseSeq(inputPath:String, startYear:Int):Unit = {
    val f0 = new File(inputPath)
    val allGzs = f0.listFiles().filter(_.getName.toLowerCase().endsWith(".gz"))
    allGzs.foreach { f =>
      val fn = f.getName
      val gzStrm = new GzipCompressorInputStream(new FileInputStream(f))
      //gzStrm.getNextEntry
      val xmlStr = IOUtils.toString(gzStrm, StandardCharsets.UTF_8)
      gzStrm.close()
      val xml = XML.loadString(xmlStr)
      val parsed = XmlReader.of[ArtiSet].read(xml)
      val artiSet:ArtiSet = parsed.getOrElse(ArtiSet.EmptyArtiSet)
      val allArtiCount = artiSet.artis.size
      val artisFiltered = artiSet.artis.filter(_.citation.journal.getPubDate.year >= startYear)
      val artiSetFiltered = artiSet.copy(artis = artisFiltered)
      val stats = ArtiStats(fn, allArtiCount, artisFiltered.size)
      println(stats)
    }
  }

  def main(args:Array[String]):Unit = {
    val runLocally = if (args.length > 0) args(0).toBoolean else true
    val inputPath = "/media/sf_vmshare/pmjs/pmds/" // if (args.length > 1) args(1) else "file:///media/sf_vmshare/pmjs/pmds/*.gz"
   // val outputPath = if (args.length > 2) args(2) else "file:///media/sf_vmshare/pmjs/_pmj"
    val parts = 4 // if (args.length > 3) args(3).toInt else 4
    val startYear = 2013

    parseSeq(inputPath, startYear)

//    parseResults.foreach { tp =>
//      val (_, c1, _, stats) = tp
//      println(stats)
//    }

    //    import spark.implicits._
    //    val parseRes = parseResults.flatMap(_._2.artis)
    //      .repartition(parts)
    //      .filter { arti =>
    //        val date = arti.citation.journal.getPubDate
    //        val res = date.year >= 2014
    //        if (!res) {
    //          println(s"${arti.citation.pmid} (published in ${date.year}) not included!")
    //        }
    //        res
    //      }
    //      .toDF
    //    //.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    //
    //    println("Saving ...")
    //
    //    SparkUtils.del(spark.sparkContext, outputPath)
    //    println(s"$outputPath data deleted ...")
    //    parseRes.write.parquet(outputPath)
    //    println("Done ...")


    // verify result

    //    spark.read.parquet(outputPath)
    //      .as[Arti]
    //      .foreach { arti =>
    //        val date = arti.citation.journal.getPubDate
    //        val res = date.year >= 2014
    //        if (!res) {
    //          println(s"${arti.citation.pmid} (published in ${date.year}) not included!")
    //        }
    //      }


  }

}
