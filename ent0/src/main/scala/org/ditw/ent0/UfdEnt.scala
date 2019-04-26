package org.ditw.ent0
import org.ditw.common.ResourceHelpers
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.{GNCntry, GNEnt}
import org.ditw.ent0.UfdType.UfdType
import org.ditw.exutil1.naen.NaEnData.NaEnCat
import org.ditw.exutil1.naen.{NaEn, NaEnData}
import org.ditw.exutil1.naenRepo.NaEnRepo

//case class UfdEnt(
//                   id:Long,
//                   isni:Option[String]
//                 ) {
//
//}

object UfdEnt extends Serializable {
  private def pieceId(
//    typePart:Int,
    cntryPart:Int,
    admsPart:Int,
//    orgFlags:Int,
    org:Int
  ):Long = {
//    var x = (typePart.toLong << 3) << 9
//    x = x | cntryPart
    var x = cntryPart.toLong

    x = x << 16
    x = x | admsPart

//    x = x << 4
//    x = x | orgFlags

    x = x << 36
    x = x | org

    x
  }

  import UfdType._
  private val Type2IdPartMap = Map(
    TODO -> 7,
    Rsvd -> 0,
    Educational -> 1,
    Operational -> 2,
    Industrial -> 3,
    Researching -> 4
  )
  import org.ditw.demo1.gndata.GNCntry._
  private val Cntry2IdPartMap = Map(
    US -> 1,
    PR -> 2,
    CA -> 4,
    GB -> 5,
    AU -> 6
  )
  private def _ufdId(
//    t:UfdType,
    cntry:GNCntry,
    admEnc:Int,
//    orgFlag:Int,
    orgLId:Long
  ):Long = {
    pieceId(
//      Type2IdPartMap(t),
      Cntry2IdPartMap(cntry),
      admEnc,
      //orgFlag,
      orgLId.toInt
    )
  }

//  private val OrgFlag_Normal = 0x1
//  private val OrgFlag_Terminated = 0x2
//  private val OrgFlag_Migrated = 0x3
  def ufdIdNormal(
                   cntry:GNCntry,
                   admEnc:Int,
                   orgLId:Long
                 ):Long = {
    _ufdId(cntry, admEnc, orgLId)
  }
  private type AdmCodeXtrer = GNEnt => Array[String]
  private val adm1Xtrer:AdmCodeXtrer = ent => ent.admCodes.slice(0, 1).toArray
  private val admCodeXtrerMap = Map[GNCntry, AdmCodeXtrer](
    US -> adm1Xtrer,
    PR -> adm1Xtrer
  )

  def ent2UfdGeo(ent: GNEnt):UfdGeo = {
    val cntry = GNCntry.withName(ent.countryCode)
    val admCodes = admCodeXtrerMap(cntry)(ent)
    UfdGeo(cntry, admCodes, ent.gnid)
  }

  private val catBits = 36
  private val catMask = -1L << catBits
  import org.ditw.common.GenUtils._

  def ufdRepoFromRes(resPath:String):NaEnRepo[NaEn] = {
    val ents = ResourceHelpers.load(resPath, NaEn.fromJsons).toVector
    ufdRepo(ents)
  }
  def ufdRepoFrom(path:String):NaEnRepo[NaEn] = {
    val ents = readJson(path, NaEn.fromJsons).toVector
    ufdRepo(ents)
  }
  private def ufdRepo(ufdEnts:Iterable[NaEn]): NaEnRepo[NaEn] = {
    //val ents = readJson(path, NaEn.fromJsons).toVector

    new NaEnRepo[NaEn](
      ufdEnts,
      "Ufd entities",
      id => id & catMask,
      newEnt => newEnt.neid & catMask,
      (newEntid, newEnt) => newEnt.copy(neid = newEntid),
      NaEnData.catIdStart(NaEnCat.UFD)
    )
  }
}