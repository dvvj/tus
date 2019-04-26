package org.ditw.sparkRuns
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.demo1.gndata.GNCntry.GB
import org.ditw.demo1.gndata.{GNEnt, GNSvc}
import org.ditw.demo1.gndata.SrcData.tabSplitter
import org.ditw.exutil1.poco.PoPfxGB
import org.ditw.sparkRuns.UtilPocoCsv1.minMax

import scala.collection.mutable.ListBuffer

object UtilPoco1Map2GNs {


  private def removeBrackets(in:String):String = {
    val rightIdx = in.lastIndexOf(')')
    if (rightIdx >= 0) {
      val leftIdx = in.lastIndexOf('(', rightIdx)
      if (leftIdx >= 0)
        in.substring(0, leftIdx).trim
      else in
    }
    else in
  }

  import CommonUtils._
  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()

    val gnLines = spSess.sparkContext.textFile(
      //"/media/sf_vmshare/fp2Affs_uniq"
      "file:///media/sf_vmshare/gns/all"
    ).map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val ccs = Set(
      GB
      //,CA , GB, AU //,FR,DE,ES,IT
    )
    val svc = GNSvc.loadNoPopuReq(gnLines, ccs)
    val ents = svc.entsByName(GB, "Hythe")
    println(ents.length)
    val brSvc = spSess.sparkContext.broadcast(svc)

    val headers = "Postcode,Latitude,Longitude,Easting,Northing,Grid Ref,Postcodes,Active postcodes,Population,Households,Built up area"
      .split(",")

    val rows = spSess.read
      .format("csv")
      .option("header", "true")
      .load("/media/sf_vmshare/postcode sectors.csv")

    import collection.mutable
    val grouped = rows.select("Postcode", "Latitude", "Longitude", "Built up area")
      .filter(_.get(3) != null)
      .rdd.map(r => removeBrackets(r.getAs[String](3)) -> (r.getAs[String](0), r.get(1).toString.toDouble, r.get(2).toString.toDouble))
      .groupByKey()
      .map { p =>
        val (name, l) = p
        val allEnts = brSvc.value.entsByName(GB, name)
        if (allEnts.nonEmpty) {
          val matchedMap = mutable.Map[Long, mutable.Set[String]]()
          l.foreach { tp =>
            val (poco, lat, lon) = tp
            val nearestEnt = allEnts.minBy(ent => distByCoord(ent.latitude, ent.longitude, lat, lon))
            if (!checkCoord(nearestEnt.latitude, nearestEnt.longitude, lat, lon)) {
              println(s"Nearest ent: [$nearestEnt] is not close enough ($lat, $lon), ignored!")
            }
            else {
              if (!matchedMap.contains(nearestEnt.gnid))
                matchedMap += nearestEnt.gnid -> mutable.Set[String]()
              matchedMap(nearestEnt.gnid) += poco
            }
          }
          PoPfxGB(name, matchedMap.mapValues(_.toVector.sorted).toMap)
        }
        else
          PoPfxGB(name, Map[Long, Vector[String]]())
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val nonEmpty = grouped.filter(_.gnid2Pfxs.nonEmpty)
    println(s"Non Empty: ${nonEmpty.count()}")
    val empty = grouped.filter(_.gnid2Pfxs.isEmpty)
    println(s"Empty: ${empty.count()}")
    empty.foreach { emp => println(emp.name) }

//    var path = "/media/sf_vmshare/pocoJson"
//    SparkUtils.del(spSess.sparkContext, path)
    val jsons = nonEmpty.sortBy(_.name.toLowerCase()).collect
    val jsonStr = PoPfxGB.toJsons(jsons)
    val saveFile = new FileOutputStream("/media/sf_vmshare/pocogb.json")
    IOUtils.write(jsonStr, saveFile, StandardCharsets.UTF_8)
    saveFile.close()

    val path = "/media/sf_vmshare/pocoTrace"
    SparkUtils.del(spSess.sparkContext, path)
    nonEmpty.map { poPfx =>
        val tr = poPfx.gnid2Pfxs.map(p => s"${p._1}: ${p._2.mkString(",")}")
          .mkString("\t", "\n\t", "")
        s"${poPfx.name}\n$tr"
      }
      .saveAsTextFile(path)

    spSess.stop()
  }
}
