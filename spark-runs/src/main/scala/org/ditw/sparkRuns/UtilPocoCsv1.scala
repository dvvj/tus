package org.ditw.sparkRuns
import org.apache.spark.storage.StorageLevel
import org.ditw.common.SparkUtils
import org.ditw.demo1.gndata.GNCntry.{JP, US, GB}
import org.ditw.demo1.gndata.{GNCntry, GNSvc}
import org.ditw.demo1.gndata.SrcData.tabSplitter

object UtilPocoCsv1 {

  private def minMax(v:Double, currMinMax:(Double, Double)):(Double, Double) = {
    val min = math.min(v, currMinMax._1)
    val max = math.max(v, currMinMax._2)
    min -> max
  }
  private def minMax(minMax1:(Double, Double), minMax2:(Double, Double)):(Double, Double) = {
    val min = math.min(minMax1._1, minMax2._1)
    val max = math.max(minMax1._2, minMax2._2)
    min -> max
  }

  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()

    val headers = "Postcode,Latitude,Longitude,Easting,Northing,Grid Ref,Postcodes,Active postcodes,Population,Households,Built up area"
      .split(",")

    import collection.mutable
    val rows = spSess.read
      .format("csv")
      .option("header", "true")
      .load("/media/sf_vmshare/postcode sectors.csv")

    val grouped = rows.select("Postcode", "Latitude", "Longitude", "Built up area")
      .filter(_.get(3) != null)
      .rdd.map(r => r.get(3) -> (r.getAs[String](0), r.get(1).toString.toDouble, r.get(2).toString.toDouble))
      .aggregateByKey(
        (
          mutable.Set[String](),
          (Double.MaxValue, Double.MinValue),
          (Double.MaxValue, Double.MinValue)
        )
      )(
        (agg1, p1) => {
          try {
            val (latMin, latMax) = minMax(p1._2, agg1._2)
            val (lonMin, lonMax) = minMax(p1._3, agg1._3)
            (
              agg1._1 + p1._1,
              (latMin, latMax),
              (lonMin, lonMax)
            )
          }
          catch {
            case t:Throwable => {
              println(s"err: $agg1, $p1")
              throw t
            }
          }
        },
        (agg1, agg2) => {
          val (latMin, latMax) = minMax(agg1._2, agg2._2)
          val (lonMin, lonMax) = minMax(agg1._3, agg2._3)
          (
            agg1._1 ++ agg2._1,
            (latMin, latMax),
            (lonMin, lonMax)
          )

        }
      )
      .mapValues { tp =>
        val sorted = tp._1.toVector.sorted
        (sorted, tp._2, tp._3)
      }
      .sortBy(_._2._1.length, false)
      .map{ tp =>
        val (name, p) = tp
        val (pocos, latMinMax, lonMinMax) = p
        (name, latMinMax, lonMinMax, pocos.mkString(","))
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val groupStr = grouped.map { p =>
      val (name, latMinMax, lonMinMax, pocos) = p
      s"$name|$latMinMax|$lonMinMax|$pocos}"
    }
    val path = "/media/sf_vmshare/poco"
    SparkUtils.del(spSess.sparkContext, path)
    groupStr.saveAsTextFile(path)

    val diff = grouped.map { p =>
      val (name, latMinMax, lonMinMax, pocos) = p
      val latDiff = latMinMax._2 - latMinMax._1
      val lonDiff = lonMinMax._2 - lonMinMax._1
      (name, latDiff, lonDiff)
    }

    val maxDiff = 0.25
    val maxLatDiff = diff.sortBy(_._2, false)
        .filter(_._2 > maxDiff).collect()
    print(maxLatDiff(0))
    val maxLonDiff = diff.sortBy(_._3, false)
      .filter(_._3 > maxDiff).collect()
    print(maxLonDiff(0))


    spSess.stop()
  }
}
