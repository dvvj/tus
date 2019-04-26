package org.ditw.sparkRuns
import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{InputHelpers, SparkUtils}
import org.ditw.textSeg.catSegMatchers.Cat2SegMatchers
import org.ditw.textSeg.common.AllCatMatchers._
import org.ditw.textSeg.common.Tags
import org.ditw.textSeg.common.Vocabs.allWords
import org.ditw.tknr.TknrHelpers.TknrTextSeg

object SegMatcherRuns extends App {

  val spark = SparkUtils.sparkContextLocal()

  val affLines = spark.textFile(
    //"/media/sf_vmshare/fp2Affs_uniq"
    "/media/sf_vmshare/aff-w2v"
  )

  val dict = InputHelpers.loadDict(allWords)
  val mmgr = mmgrFrom(
    dict,
    Cat2SegMatchers.segMatchers(dict)
  )
  val brMgr = spark.broadcast(mmgr)
  println(s"Line count: ${affLines.count}")

  val brEndingChars = spark.broadcast(Set(",", ";"))

  val allUnivs = affLines.map { l =>
    val mp = run(brMgr.value, l, dict,
      TknrTextSeg()
    )
    val matches = mp.get(Tags.TagGroup4Univ.segTag)
    val univNames = matches.map { m =>
      var t = m.range.origStr
      if (brEndingChars.value.exists(t.endsWith))
        t = t.substring(0, t.length-1)
      t
    }
    univNames.map(_ -> l)
//    val sorted = matches.toList.sortBy(_.range)
//    println(sorted.size)
//    sorted
  }.filter(_.nonEmpty)
    .cache()

  println(s"Matched Line count: ${allUnivs.count}")

  val s = allUnivs
    .flatMap(s => s)
    .groupByKey()
    .sortBy(x => x._1)
    .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  val savePath = "/home/dev/univs"
  SparkUtils.deleteLocal(savePath)
  s.mapValues(l => s"\t(${l.size})\n" + l.mkString("\t", "\n\t", ""))
    .map { p =>
      s"${p._1}\n${p._2}"
    }
    .saveAsTextFile(savePath)

  val savePath2= "/home/dev/univs-count"
  SparkUtils.deleteLocal(savePath2)
  s.map { p =>
      f"${p._2.size}%05d, ${p._1}"
    }
    .saveAsTextFile(savePath2)

  spark.stop()

}
