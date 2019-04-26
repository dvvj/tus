package org.ditw.sparkRuns.entUtil
import org.ditw.common.{GenUtils, SparkUtils}
import org.ditw.demo1.gndata.GNCntry.US
import org.ditw.ent0.UfdEnt
import org.ditw.exutil1.naen.NaEn
import org.ditw.sparkRuns.CommonUtils.loadGNMmgr

object NewEntUtil {

  private val IdTBD = 0L

  def main(args:Array[String]):Unit = {
    val spSess = SparkUtils.sparkSessionLocal()
    val ccs = Set(US)
    val gnmmgr = loadGNMmgr(
      ccs,
      Set(),
      spSess.sparkContext,
      "file:///media/sf_vmshare/gns/all")

    val brGNMmgr = spSess.sparkContext.broadcast(gnmmgr)

    val adm1Maps = gnmmgr.svc.adm1Maps

    val orig = UfdEnt.ufdRepoFrom("/media/sf_vmshare/ufd-isni.json")
//    val admc = adm1Maps(US)("US_MS")
//    val id = UfdEnt.ufdIdNormal(US, admc, IdTBD)
//
//    val te = NaEn(
//      id,
//      "Mississippi State University",
//      List(),
//      4447161
//    )

    val newEnts = GenUtils.readJson("/media/sf_vmshare/new-ent-us.json", NewEntInfo.load)

    val ents = newEnts.map { ne =>
      val admc = adm1Maps(US)(ne.adm1)
      val id = UfdEnt.ufdIdNormal(US, admc, IdTBD)
      val ent = NaEn(id, ne.name, ne.aliases.toList, ne.gnid)
      ent
    }

    ents.foreach(orig.add)

    val resEnts = orig.allEnts.toArray.sortBy(_.neid)
    import org.ditw.common.GenUtils._
    writeJson("/media/sf_vmshare/tmpadd-ufd-isni.json", resEnts, NaEn.toJsons)

//    orig.traceCat(id)
  }
}
