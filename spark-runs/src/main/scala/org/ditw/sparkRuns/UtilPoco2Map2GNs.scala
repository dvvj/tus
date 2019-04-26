package org.ditw.sparkRuns
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{Dict, SparkUtils}
import org.ditw.demo1.gndata.GNCntry.{PR, US}
import org.ditw.demo1.gndata.GNSvc
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.extract.XtrMgr
import org.ditw.exutil1.poco.{PoPfxGB, PocoUS}
import org.ditw.matcher.{MatchPool, MatcherMgr}
import org.ditw.textSeg.common.Tags.TagGroup4Univ
import org.ditw.tknr.TknrHelpers
import org.ditw.tknr.Tokenizers.TTokenizer


object UtilPoco2Map2GNs {

  def main(args:Array[String]):Unit = {
    val spark = SparkUtils.sparkContextLocal()

    import CommonUtils._
    val gnmmgr = loadGNMmgr(Set(US, PR), Set(PR), spark, "file:///media/sf_vmshare/gns/all")

    val brGNMmgr = spark.broadcast(gnmmgr)

    //country code      : iso country code, 2 characters
    //postal code       : varchar(20)
    //place name        : varchar(180)
    //admin name1       : 1. order subdivision (state) varchar(100)
    //admin code1       : 1. order subdivision (state) varchar(20)
    //admin name2       : 2. order subdivision (county/province) varchar(100)
    //admin code2       : 2. order subdivision (county/province) varchar(20)
    //admin name3       : 3. order subdivision (community) varchar(100)
    //admin code3       : 3. order subdivision (community) varchar(20)
    //latitude          : estimated latitude (wgs84)
    //longitude         : estimated longitude (wgs84)
    //accuracy          : accuracy of lat/lng from 1=estimated to 6=centroid
    //--------------------
    //US	10474	Bronx	New York	NY	Bronx	005			40.8139	-73.8841	4
    //US	10475	Bronx	New York	NY	Bronx	005			40.8729	-73.8278	4
    //US	13737	Bible School Park	New York	NY	Broome	007			42.0805	-76.0973	1
    //US	13744	Castle Creek	New York	NY	Broome	007			42.2568	-75.9087	4

    val grouped = spark.textFile("/media/sf_vmshare/gns/usPostals.txt")
      .map { l =>
        val parts = tabSplitter.split(l)
        val postal = parts(1)
        val placeAdm1Name = parts(2) -> parts(4)
        val coord = parts(9).toDouble -> parts(10).toDouble
        placeAdm1Name -> (postal, coord)
      }
      .groupByKey()
      //.mapValues(_.map(_.))
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val t = grouped.flatMap { p =>
      val (placeAdm1Name, postalCoord) = p
      val placeAdm1 = s"${placeAdm1Name._1}, ${placeAdm1Name._2}"

//      if ("Washington, District of Columbia" == placeAdm1)
//        println("ok")
      val gnm = brGNMmgr.value
      var rng2Ents = runStr(
        placeAdm1,
        gnm.tknr, gnm.dict, gnm.mmgr, gnm.svc,
        gnm.xtrMgr,
        true
      )
      if (rng2Ents.isEmpty) {
        val pfx2Repl = Pfx2Replace.keySet.filter(placeAdm1.startsWith)
        if (pfx2Repl.nonEmpty) {
          if (pfx2Repl.size > 1) throw new IllegalArgumentException("todo")
          val pfx = pfx2Repl.head
          val placeAdm1Repl = Pfx2Replace(pfx) + placeAdm1.substring(pfx.length)
          rng2Ents = runStr(
            placeAdm1Repl,
            gnm.tknr, gnm.dict, gnm.mmgr, gnm.svc,
            gnm.xtrMgr,
            true
          )
        }
      }

      if (rng2Ents.isEmpty) {
        println(s"none:$placeAdm1")
        None
      }
      else {
        if (rng2Ents.size == 1) {
          val ents = rng2Ents.values.head
//          if (ents.size > 1) {
//            println(s"more than one entity:$placeAdm1")
//          }

          postalCoord.flatMap { pc =>
            val nearest = ents
              .minBy(ent => distByCoord(ent.latitude, ent.longitude, pc._2._1, pc._2._2))
            if (!checkCoord(nearest.latitude, nearest.longitude, pc._2._1, pc._2._2)) {
              val nearestCoord = (nearest.latitude, nearest.longitude)
              val currCoord = pc._2
              val diff = f"(${currCoord._1-nearestCoord._1}%.2f,${currCoord._2-nearestCoord._2}%.2f)"
              println(s"too far [$placeAdm1] $diff: ${nearest.gnid} $nearestCoord vs. ${pc._1} $currCoord)")
              None
            }
            else {
              Option(placeAdm1 -> (nearest.gnid, pc))
            }
          }

        }
        else {
          val ranges = rng2Ents.keySet.toList.sortBy(r => r.end-r.start).mkString("||")
          println(s"more than one ranges:$placeAdm1 ($ranges)")
          None
        }
      }

    }

    println(s"#: ${t.count()}")

    val res = t.groupByKey()
      .mapValues { pcs =>
        pcs.map(p => p._1 -> p._2._1)
          .groupBy(_._1)
          .mapValues(_.map(_._2).toVector)
          .map(identity)
      }
      .map { p =>
        PocoUS(p._1, p._2)
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val pocos = res.collect()
    spark.stop()

    import org.ditw.common.GenUtils._

    writeJson(
      "/media/sf_vmshare/pocoUSJson.txt",
      pocos, PocoUS.toJsons
    )
  }


  private val Pfx2Replace = Map(
    "Mc " -> "Mc",
    "La " -> "La"
  )
}
