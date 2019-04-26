package org.ditw.demo1.gndata
import org.apache.spark.rdd.RDD
import org.ditw.common.TkRange
import org.ditw.demo1.gndata.GNCntry.{CA, GNCntry, US}
import org.ditw.demo1.gndata.SrcData.{loadAdm0, loadCountries}
import org.ditw.demo1.src.SrcDataUtils
import org.ditw.extract.XtrMgr
import org.ditw.matcher.MatchPool

import scala.collection.mutable.ListBuffer

class GNSvc private (private[demo1] val _cntryMap:Map[GNCntry, TGNMap]) extends Serializable {
  private val idMap:Map[Long, GNEnt] = {
    _cntryMap.flatMap(_._2.idMap)
  }

  def adm1Maps:Map[GNCntry, Map[String, Int]] = _cntryMap.map { p =>
    p._1 -> {
      val adm1s = p._2.directSubAdms
      adm1s.indices.map { idx =>
        adm1s(idx) -> (idx+1)
      }.toMap
    }
  }

  def entById(gnid:Long):Option[GNEnt] = {
    idMap.get(gnid)
  }

  def entsByName(cntry: GNCntry, name:String):IndexedSeq[GNEnt] = {
    _cntryMap(cntry).byName(name)
  }

  import GNSvc._
  def extrEnts(xtrMgr:XtrMgr[Long], matchPool: MatchPool):Map[TkRange, List[GNEnt]] = {
    val xtrs = xtrMgr.run(matchPool)
    val t = xtrs.mapValues(_.flatMap(entById))

    val res = t.map(p => p._1 -> mergeEnts(p._2))

    res
  }
}

object GNSvc extends Serializable {

  private[demo1] def checkContains(ent1:GNEnt, ent2:GNEnt):Boolean = {
    if (ent1.countryCode == ent2.countryCode &&
      ent1.admCodes.length <= ent2.admCodes.length
    ) {
      val admCodesChecked = ent1.admCodes.indices.forall(
        idx => ent1.admCodes(idx) == ent2.admCodes(idx)
      )
      if (!admCodesChecked) false
      else if (ent1.admCodes.length < ent2.admCodes.length && ent1.isAdm) true
      else {
        if (ent1.isAdm && !ent2.isAdm) true
        else false
      }
    }
    else false
  }

  private[demo1] def checkIfContains(ent1:GNEnt, ent2:GNEnt):Option[GNEnt] = {
    if (ent1.countryCode == ent2.countryCode &&
      ent1.admCodes.length <= ent2.admCodes.length
    ) {
      val admCodesChecked = ent1.admCodes.indices.forall(
        idx => ent1.admCodes(idx) == ent2.admCodes(idx)
      )
      if (!admCodesChecked) None
      else if (ent1.admCodes.length < ent2.admCodes.length) Option(ent2)
      else {
        val ent1IsAdm = SrcDataUtils.isAdm(ent1)
        val ent2IsAdm = SrcDataUtils.isAdm(ent2)
        if (ent1IsAdm && !ent2IsAdm) Option(ent2)
        else if (ent1IsAdm && ent2IsAdm) {
          println(s"both adms?! $ent1 -- $ent2")
          None
        }
          // throw new IllegalArgumentException("both adms?!")
        else Option(ent1)
      }
    }
    else None
  }

  private[gndata] def mergeEnts(ents:List[GNEnt]):List[GNEnt] = {
    if (ents.size <= 1)
      ents
    else {
      var head = ents.head
      var tail = ents.tail
      val res = ListBuffer[GNEnt]()
      while (tail.nonEmpty) {
        val itTail = tail.iterator
        var headMerged = false
        val remTail = ListBuffer[GNEnt]()
        while (!headMerged && itTail.hasNext) {
          val t = itTail.next()
          val c = checkIfContains(head, t)
          if (c.nonEmpty) {
            if (c.get.eq(t)) {
              headMerged = true
              remTail += t
            }
            else {
              // tail merged, do not add to remTail
            }
          }
          else {
            remTail += t
          }
        }
        remTail ++= itTail

        if (!headMerged) res += head
        if (remTail.size > 1) {
          head = remTail.head
          tail = remTail.tail.toList
        }
        else {
          if (remTail.size == 1)
            res += remTail.head  // the remaining one
          tail = Nil
        }
      }
      res.toList
    }

  }

  def load(
    lines:RDD[Array[String]],
    countries:Set[GNCntry],
    settings: LoadSettings
  ): GNSvc = {
    val spark = lines.sparkContext
    val adm0Ents = loadAdm0(lines)
    val brAdm0Ents = spark.broadcast(adm0Ents)

//    val ccs = Set(
//      US, CA
//      //, "GB", "AU", "FR", "DE", "ES", "IT"
//    )
    val adm0s = loadCountries(lines, countries, brAdm0Ents, settings)

    new GNSvc(adm0s)
  }

  case class LoadSettings(minPopu:Long)

  private val MinPopu = 500
  private val AllPopu = -1
  val defSettings = LoadSettings(MinPopu)
  val noPopuSettings = LoadSettings(AllPopu)

  def loadDef(
    lines:RDD[Array[String]],
    countries:Set[GNCntry]
  ): GNSvc = {
    load(lines, countries, defSettings)
  }

  def loadNoPopuReq(
    lines:RDD[Array[String]],
    countries:Set[GNCntry]
  ): GNSvc = {
    load(lines, countries, noPopuSettings)
  }

}