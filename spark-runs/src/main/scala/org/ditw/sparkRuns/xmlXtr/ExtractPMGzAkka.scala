package org.ditw.sparkRuns.xmlXtr
import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.lucidchart.open.xtract.XmlReader
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.ditw.common.GenUtils
import org.ditw.pmxml.model.{AAAuAff, ArtiSet}
import org.ditw.sparkRuns.xmlXtr.ExtractPMGzPqtUtils.ArtiStats

import scala.concurrent.Future
import scala.xml.XML

object ExtractPMGzAkka {
  def main(args:Array[String]):Unit = {

    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val sys = ActorSystem("sys")
    implicit val mat = ActorMaterializer()
    val startYear = 2014
    val allFiles = new File("/media/sf_vmshare/pmjs/pmds/")
      .listFiles()
      .filter(_.getName.toLowerCase().endsWith(".gz"))
      .toList.sortBy(_.getName)(Ordering[String].reverse)

    val outFolder = "/media/sf_vmshare/pmjs/pmds_older_out/"
    val outF = new File(outFolder)
    if (!outF.exists())
      outF.mkdirs()

    val filesSrc:Source[File, NotUsed] = Source.fromIterator(() => allFiles.iterator)
    val suffix = ".xml.gz"
    val prefix = "pubmed19n"

    val xtrFlow:Flow[File, String, NotUsed] = Flow[File].map { f =>
      val fn = f.getName
      val idx = fn.substring(0, fn.length-suffix.length).substring(prefix.length).toInt
      val gzStrm = new GzipCompressorInputStream(new FileInputStream(f))
      //gzStrm.getNextEntry
      val xmlStr = IOUtils.toString(gzStrm, StandardCharsets.UTF_8)
      gzStrm.close()
//      println(xmlStr.length)
//      fn
//      try {
        val xml = XML.loadString(xmlStr)
        val parsed = XmlReader.of[ArtiSet].read(xml)
        val artiSet:ArtiSet = parsed.getOrElse(ArtiSet.EmptyArtiSet)
        val allArtiCount = artiSet.artis.size
        val artisFiltered = artiSet.artis.filter { arti =>
          try {
            arti.citation.getArticleDate.year >= startYear
          }
          catch {
            case t:Throwable => {
              println(s"!! ${arti.citation.pmid}: ${t.getMessage}")
              false
            }
          }
        }
        val artiSetFiltered = artiSet.copy(artis = artisFiltered)
        val stats = ArtiStats(fn, allArtiCount, artisFiltered.size)
        //println(stats)
//        if (idx < 909 || idx > 959)
//          println(s"$fn: ${stats.filteredCount}")

        if (stats.filteredCount > 0) {
          val outFn = fn.substring(0, fn.length-suffix.length) + ".jsons"
          val outAuAffs = artiSetFiltered.artis.map { art =>
            val pmid = art.citation.pmid
            val pubYear = art.citation.getArticleDate.year
            val auAff = AAAuAff.createFrom(art.citation.authors)
            val auAffStr = AAAuAff.toJson(auAff)
            (pmid, pubYear, auAffStr)
          }
          val outStr = outAuAffs.mkString("\n") //ArtiSet.toJson(artiSetFiltered)
          GenUtils.writeStr(s"$outFolder$outFn", outStr)
        }
        else {
          println(s"Empty file: ${stats.name}")
        }
        stats.toString
//      }
//      catch {
//        case t:Throwable => {
//          println(s"error! ${fn}: ${t.getMessage}")
//          //throw t
//        }
//      }
    }

    val xtrSrc = filesSrc.via(xtrFlow)

    val consoleSink:Sink[String, Future[Done]] = Sink.foreach[String](println)

    val done:Future[Done] = xtrSrc.runWith(consoleSink)

    done.onComplete(_ => sys.terminate())

  }
}
