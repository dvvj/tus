package org.ditw.sparkRuns.csvXtr
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{GenUtils, SparkUtils}
import org.ditw.demo1.gndata.GNCntry.{PR, US}
import org.ditw.exutil1.naen.NaEnData.NaEnCat
import org.ditw.exutil1.naen.{NaEn, NaEnData, SrcCsvMeta}
import org.ditw.sparkRuns.csvXtr.UtilsEntCsv1.{dash, dashTokenSet, isDash, spaceSplitter}
import org.ditw.sparkRuns.{CommonCsvUtils, CommonUtils}

object UtilsEntCsv2 extends Serializable {

  val headers =
    "X,Y,OBJECTID,IPEDSID,NAME,ADDRESS,CITY,STATE,ZIP,ZIP4," +
      "TELEPHONE,TYPE,STATUS,POPULATION,COUNTY,COUNTYFIPS,COUNTRY," +
      "LATITUDE,LONGITUDE,NAICS_CODE,NAICS_DESC,SOURCE,SOURCEDATE," +
      "VAL_METHOD,VAL_DATE,WEBSITE,STFIPS,COFIPS,SECTOR,LEVEL_," +
      "HI_OFFER,DEG_GRANT,LOCALE,CLOSE_DATE,MERGE_ID,ALIAS," +
      "SIZE_SET,INST_SIZE,PT_ENROLL,FT_ENROLL,TOT_ENROLL,HOUSING," +
      "DORM_CAP,TOT_EMP,SHELTER_ID"

  private val csvMeta = SrcCsvMeta(
    "NAME",
    "ALIAS",
    Option("LATITUDE", "LONGITUDE"),
    Vector("CITY", "STATE")
  )

  private def rowInfo(row:Row):String = {
    row.getAs[String](csvMeta.nameCol)
  }

  import EntXtrUtils._
  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()


    import CommonUtils._
    val gnmmgr = loadGNMmgr(Set(US, PR), Set(PR), spSess.sparkContext, "file:///media/sf_vmshare/gns/all")

    val brGNMmgr = spSess.sparkContext.broadcast(gnmmgr)

    val rows = csvRead(
      spSess,
      "/media/sf_vmshare/Colleges_and_Universities.csv",
      csvMeta
    )
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    val idStart = NaEnData.catIdStart(NaEnCat.US_UNIV)
    import CommonCsvUtils._

    type RowResType = (String, List[String], Long, Map[String, String])
    val (ents, errors) = process[RowResType](
      rows,
      row => {
        val cityState = csvMeta.gnStr(row)
        val rng2Ents = extrGNEnts(cityState, brGNMmgr.value, true, Pfx2Replace)

        var errMsg:Option[String] = None
        var res:Option[RowResType] = None
        if (rng2Ents.isEmpty) {
          errMsg = taggedErrorMsg(1, s"$cityState not found")
        }
        else {
          val nearest = checkNearestGNEnt(rng2Ents.values.flatten, row, csvMeta.latCol, csvMeta.lonCol)

          if (nearest.nonEmpty) {
            val name = csvMeta.name(row)
            val altName = csvMeta.altNames(row)
            val altNames =
              if (altName == null || altName.isEmpty || altName == "NOT AVAILABLE")
                List[String]()
              else List(altName)
            val (processedName, ex) = processName(name)
            val attrs = if (ex.nonEmpty) {
                Map("Ex" -> ex.get)
              }
              else Map[String, String]()
            res = Option((processedName, altNames, nearest.get.gnid, attrs))
          }
          else {
            val coord = csvMeta.getCoord(row)
            val coordTr = f"(${coord._1}%.4f,${coord._2}%.4f)"
            errMsg = taggedErrorMsg(2, s"Nearest not found for $cityState$coordTr, candidates: $rng2Ents")
          }
        }
        (rowInfo(row), res, errMsg)
      },
      (tp, idx) => {
        val (name, alias, gnid, attrs) = tp._2.get
        val id = idStart + idx
        NaEn(id, name, alias, gnid, attrs)
      }
    )
//    val res = rows.rdd.map
//      .sortBy(_._1)
//      .zipWithIndex()
//      .map
//      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//    println(s"#: ${res.count()}")
//
//    val hosps = res.collect()
    import GenUtils._
    writeJson(
      "/media/sf_vmshare/us_univ_coll.json",
      ents, NaEn.toJsons
    )
    val errorOut = new FileOutputStream("/media/sf_vmshare/us_univ_coll_err.txt")
    IOUtils.write(errors.mkString("\n"), errorOut, StandardCharsets.UTF_8)
    errorOut.close()

    spSess.close()
  }

  private val spaceSplitter = "\\s+".r
  private val dash = '-'
  private val LocIndicators = Set("at", "-")
  private def isLocIndicator(s:String):Boolean = LocIndicators.contains(s)
  private def isDash(s:String):Boolean = s == "-"

  private def processName(name:String):(String, Option[String]) = {
    val spaceSplits = spaceSplitter.split(name.toLowerCase())
    val tokens = spaceSplits.flatMap { s =>
      if (!isDash(s)) {
        GenUtils.splitPreserve(s, dash)
      }
      else
        List(s)
    }
    val firstDash = tokens.indices.find(idx => isLocIndicator(tokens(idx)))
    if (firstDash.nonEmpty) {
      val s = tokens.slice(0, firstDash.get)
      if (isValidUnivName(s)) {
        val p1 = s.mkString(" ")
        val p2 = tokens.slice(firstDash.get+1, tokens.length)
        p1 -> Option(p2.mkString(" "))
      }
      else // probably a wrong slice
        name.toLowerCase() -> None
    }
    else name.toLowerCase() -> None
  }

  private val validUnivEnding = Set(
    "university"
  )

  private def isValidUnivName(tokens:Array[String]):Boolean = {
    if (tokens.length > 2) true
    else if (tokens.length == 2) {
      if (validUnivEnding.contains(tokens(1).toLowerCase())) true
      else false
    }
    else false
  }

  private val Pfx2Replace = Map(
    "ST " -> "SAINT ",
    "ST. " -> "SAINT ",
    "MT. " -> "MOUNT ",
    "MC " -> "MC"
  )
}
