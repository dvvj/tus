package org.ditw.sparkRuns.csvXtr
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{GenUtils, ResourceHelpers, SparkUtils}
import org.ditw.demo1.gndata.GNCntry
import org.ditw.exutil1.naen.NaEnData.NaEnCat
import org.ditw.exutil1.naen.{NaEn, NaEnData, SrcCsvMeta}
import org.ditw.sparkRuns.csvXtr.EntXtrUtils.process
import org.ditw.sparkRuns.csvXtr.UtilsEntCsv2.csvMeta
import org.ditw.sparkRuns.{CommonCsvUtils, CommonUtils}

object UtilsEntCsv1 extends Serializable {

  private val headers =
    "X,Y,OBJECTID,ID,NAME,ADDRESS," +
      "CITY,STATE,ZIP,ZIP4,TELEPHONE," +
      "TYPE,STATUS,POPULATION,COUNTY," +
      "COUNTYFIPS,COUNTRY,LATITUDE,LONGITUDE," +
      "NAICS_CODE,NAICS_DESC,SOURCE,SOURCEDATE," +
      "VAL_METHOD,VAL_DATE,WEBSITE,STATE_ID," +
      "ALT_NAME,ST_FIPS,OWNER,TTL_STAFF,BEDS,TRAUMA,HELIPAD"
  private val csvMeta = SrcCsvMeta(
    "NAME",
    "ALT_NAME",
    Option("LATITUDE", "LONGITUDE"),
    Vector("CITY", "STATE")
  )

  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()
    import GNCntry._
    val ccs = Set(US, PR)

    import CommonUtils._

    val gnmmgr = loadGNMmgr(ccs, Set(PR), spSess.sparkContext, "file:///media/sf_vmshare/gns/all")

    val brGNMmgr = spSess.sparkContext.broadcast(gnmmgr)

    val rows = csvRead(
        spSess,
      "/media/sf_vmshare/Hospitals.csv",
        csvMeta
      )
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val idStart = NaEnData.catIdStart(NaEnCat.US_HOSP)

    import CommonCsvUtils._
    import EntXtrUtils._

    type RowResType = (String, List[String], Long)
    val (ents, errors) = process[RowResType](
      rows.filter(r => csvMeta.name(r) != null),
      row => {

        val cityState = csvMeta.gnStr(row)
        val rng2Ents = extrGNEnts(cityState, brGNMmgr.value, true, Pfx2Replace)

        var errMsg:Option[String] = None
        var res:Option[RowResType] = None
        val coord = csvMeta.getCoord(row)
        val coordTr = f"(${coord._1}%.4f,${coord._2}%.4f)"
        if (rng2Ents.isEmpty) {
          errMsg = taggedErrorMsg(1, s"$cityState$coordTr not found")
        }
        else {
          val nearest = checkNearestGNEnt(rng2Ents.values.flatten, row, csvMeta.latCol, csvMeta.lonCol)
          if (nearest.nonEmpty) {
            val name = csvMeta.name(row)
            val altName = csvMeta.altNames(row)
            val altNames = if (altName == null || altName == "NOT AVAILABLE" || altName.isEmpty) //todo
              List[String]()
            else List(altName)
            res = Option((processName(name), altNames, nearest.get.gnid))
          }
          else {
            errMsg = taggedErrorMsg(2, s"Nearest not found for $cityState$coordTr, candidates: $rng2Ents")
          }
        }
        var ri = row.getAs[String](csvMeta.nameCol)
        if (ri == null)
          ri = ""
        (ri, res, errMsg)
      },
      (tp, idx) => {
        val id = idStart + idx
        val (name, altNames, gnid) = tp._2.get
        NaEn(id, name, altNames, gnid)
      }
    )

//    val res = rows.rdd.flatMap
//      .sortBy(_._1)
//      .zipWithIndex()
//      .map { p =>
//        val (tp, idx) = p
//
//      }
//      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//    println(s"#: ${res.count()}")
//
//    val hosps = res.collect()
    import GenUtils._
    writeJson(
      "/media/sf_vmshare/us_hosp.json",
      ents, NaEn.toJsons
    )
    val errorOut = new FileOutputStream("/media/sf_vmshare/us_hosp_err.txt")
    IOUtils.write(errors.mkString("\n"), errorOut, StandardCharsets.UTF_8)
    errorOut.close()
    spSess.stop()
  }

  private val dashTokenSet = ResourceHelpers.loadStrs("/hosp_name_with_dash.txt")
    .map(_.toLowerCase()).toSet
  private val spaceSplitter = "\\s+".r
  private val dash = '-'
  private def isDash(s:String):Boolean = s == "-"
  private def processName(name:String):String = {
    val spaceSplits = spaceSplitter.split(name.toLowerCase())
    val tokens = spaceSplits.flatMap { s =>
      if (dashTokenSet.contains(s)) List(s)
      else {
        if (!isDash(s)) {
          GenUtils.splitPreserve(s, dash)
        }
        else
          List(s)
      }
    }
    val firstDash = tokens.indices.find(idx => isDash(tokens(idx)))
    if (firstDash.nonEmpty) {
      val s = tokens.slice(0, firstDash.get)
      if (s.length > 2)
        s.mkString(" ")
      else // probably a wrong slice
        name.toLowerCase()
    }
    else name.toLowerCase()
  }

  private val Pfx2Replace = Map(
    "ST " -> "SAINT ",
    "ST. " -> "SAINT ",
    "MT. " -> "MOUNT ",
    "MC " -> "MC"
  )
}
