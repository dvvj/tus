package org.ditw.sparkRuns.xmlXtr
import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.ditw.pmxml.model.AAAuAff

object ExtractPMGzAkka_PostProc {
  def main(args:Array[String]):Unit = {

    import collection.mutable
    val path = new File(
      "/media/sf_vmshare/pmjs/auAff2014p" //auAff-dbg" //auAff2014p/"
    )
    val pmidsSoFar = mutable.Set[Long]()

    val allFiles = path.listFiles().filter(_.getName.endsWith(".jsons"))
      .sortBy(_.getName)(Ordering[String].reverse)

    import collection.JavaConverters._
    allFiles.foreach { f =>
      val strm = new FileInputStream(f)
      val lines = IOUtils.readLines(strm, StandardCharsets.UTF_8)
      strm.close()

      val newLines = lines.asScala.flatMap { l =>
        val firstComma = l.indexOf(",")
        val pmid = l.substring(1, firstComma).toLong
        if (pmidsSoFar.contains(pmid)) {
          println(s"${f.getName}: $pmid exists, skipping ...")
          None
        }
        else {
          Option(pmid -> l)
//          val secComma = l.indexOf(",", firstComma+1)
//          val pubYear = l.substring(firstComma+1, secComma).toInt
//          val js = l.indexOf("{")
//          val j = l.substring(js, l.length-1)
//          val auAff = AAAuAff.fromJson(j)
        }
        //pmidsSoFar += pmid
      }

      val remDup = newLines.groupBy(_._1).mapValues(_.last._2)
        .toVector.sortBy(_._1)
      pmidsSoFar ++= remDup.map(_._1)
      val changed = remDup.size != lines.size()
      if (changed) {
        println(s"Updating ${f.getName} ...")
        val outStr = remDup.map(_._2).mkString("\n")
        val of = new FileOutputStream(f)
        IOUtils.write(outStr, of, StandardCharsets.UTF_8)
        of.close()
      }
    }


  }
}
