package org.ditw.sparkRuns.csvXtr
import java.io.FileOutputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{ResourceHelpers, SparkUtils}
import org.ditw.demo1.gndata.GNCntry._
import org.ditw.demo1.gndata.{GNCntry, GNEnt}
import org.ditw.ent0.UfdEnt
import org.ditw.exutil1.naen.NaEnData.NaEnCat
import org.ditw.exutil1.naen.{NaEn, NaEnData, SrcCsvMeta}
import org.ditw.sparkRuns.CommonUtils
import org.ditw.sparkRuns.pmXtr.PmXtrUtils

object UtilsExtrUfdEnts {
  import EntXtrUtils._
  import org.ditw.ent0.UfdType._

  import IsniSchema._

  private val Blacklisted = Set(
    "Elementary School",
    "High School",
    "Middle School",
    "book store",
    "Bookstore",
    "Library"
  ).map(_.toLowerCase())
  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()
    val ccs = Set(CA)

    import CommonUtils._
    import GNCntry._
    val gnmmgr = loadGNMmgr(
      ccs,
      Set(),
      spSess.sparkContext,
      "file:///media/sf_vmshare/gns/all")

    val brGNMmgr = spSess.sparkContext.broadcast(gnmmgr)

    val adm1Maps = gnmmgr.svc.adm1Maps
    val brAdm1Maps = spSess.sparkContext.broadcast(adm1Maps)

    val rows = csvRead(
      spSess,
      "/media/sf_vmshare/ringgold_isni.csv",
      csvMeta
    ).persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val nameGnHints = ResourceHelpers.load("/isni_gnhint.json", IsniGnHint.load)
      .map { h => h.strInName.toLowerCase() -> h.gnid }.toMap
    val brNameHint = spSess.sparkContext.broadcast(nameGnHints)

    val idStart = NaEnData.catIdStart(NaEnCat.UFD)

    val EmptyEnts = List[GNEnt]()
    type RowResType = (String, GNCntry, List[String], GNEnt, Map[String, String])
    val (ents, errors) = process[RowResType](
      rows,
      row => {
        val cc = csvMeta.strVal(row, ColCountryCode)
        var res:Option[RowResType] = None
        var errMsg:Option[String] = None
        if (!GNCntry.contains(cc)) {
          errMsg = taggedErrorMsg(1, s"Country code [$cc] not found")
        }
        else if (!ccs.contains(GNCntry.withName(cc))) {
          errMsg = taggedErrorMsg(2, s"Country code [$cc] not included")
        }
        else {
          val name = csvMeta.name(row)
          val lowerName = name.toLowerCase()

          if (Blacklisted.exists(lowerName.contains)) {
            errMsg = taggedErrorMsg(-1, s"$name blacklisted")
          }
          else {
            val cityStateCC = csvMeta.gnStr(row)
            val rng2Ents = extrGNEnts(cityStateCC, brGNMmgr.value, false)
            if (rng2Ents.isEmpty) {
              errMsg = taggedErrorMsg(3, s"$cityStateCC not found")
            }
            else {
              //val nearest = checkNearestGNEnt(rng2Ents.values.flatten, row, "LATITUDE", "LONGITUDE")
              val allEnts = rng2Ents.values.flatten

              val lowerName = name.toLowerCase()
              val h = brNameHint.value.filter(p => name.toLowerCase().contains(p._1))
              var ents =
                if (h.nonEmpty) {
                  if (h.size > 1) {
                    throw new RuntimeException("todo: multiple hints?")
                  }
                  else {
                    val gnidHint = h.head._2
                    val hintEnt = brGNMmgr.value.svc.entById(gnidHint)
                    if (hintEnt.nonEmpty) {
                      val hint = allEnts.filter(PmXtrUtils._checkGNidByDist(hintEnt.get, _))
                      //println(s"Found hint for [$name]: $hint")
                      hint
                    }
                    else EmptyEnts
                  }
                }
                else allEnts

              if (ents.size > 1) {
                val maxPop = ents.map(_.population).max
                ents = ents.filter(_.population == maxPop)
              }

              if (ents.size == 1) {
                val ent = ents.head
                val altName = csvMeta.altNames(row)
                val altNames =
                  if (altName == null || altName.isEmpty)
                    List[String]()
                  else List(altName)
                res = Option((name, GNCntry.withName(cc), altNames, ent,
                  Map(
                    NaEn.Attr_CC -> cc
                    //,NaEn.Attr_ISNI -> csvMeta.strVal(row, ColISNI)
                  ) ++ csvMeta.otherKVPairs(row)
                ))
              }
              else {
                if (ents.nonEmpty) {
                  val maxPop = ents.map(_.population).max
                  errMsg = taggedErrorMsg(4, s"todo: more than one ents (max population: $maxPop): $ents")
                }
                else {
                  errMsg = taggedErrorMsg(3, s"$cityStateCC not found (filtered)")
                }
              }
            }
          }
        }
        val ri = rowInfo(row)
        (ri, res, errMsg)

      },
      (tp, idx) => {
        val orgLId = idStart + idx
        val (ri, res, _) = tp
        val (name, cntry, alts, ent, attrs) = res.get
        //val geo = UfdEnt.ent2UfdGeo(ent) // UfdGeo(cntry, Array(), ent.gnid)
        //val cntry = GNCntry.withName(ent.countryCode)
        val adm1 = s"${cntry}_${ent.admCodes.head}"
        val admc = brAdm1Maps.value(cntry)(adm1)
        //val geoEnt = brGNMmgr.value.svc.entById(geo.gnid)
        val id = UfdEnt.ufdIdNormal(cntry, admc, orgLId)
        NaEn(id, name, alts, ent.gnid, attrs)
      }
    )

    println(s"Ents: ${ents.length}")

    import org.ditw.common.GenUtils._
    writeJson(
      "/media/sf_vmshare/new-ufd-isni.json",
      ents, NaEn.toJsons
    )


    writeStr("/media/sf_vmshare/isni_err.txt", errors)

    spSess.stop()
  }



}
