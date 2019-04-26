package org.ditw.exutil1.naen
import org.ditw.common.{Dict, ResourceHelpers}
import org.ditw.common.InputHelpers.splitVocabEntries
import org.ditw.matcher.{TTkMatcher, TokenMatchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object NaEnData extends Serializable {

//  trait NaEnInfo extends Serializable {
//
//  }
  import collection.mutable

  object NaEnCat extends Enumeration {
    type NaEnCat = Value
    val US_UNIV, US_HOSP, ISNI, UFD = Value
  }

  import NaEnCat._

  val idStartCatMap:Map[NaEnCat, Long] = Map(
    US_UNIV -> 1000000000L,
    US_HOSP -> 2000000000L,
    ISNI -> 3000000000L,
    UFD -> 0x10000L
  )

  def catIdStart(cat:NaEnCat):Long = idStartCatMap(cat)


  private def splitNames(ne:NaEn):Set[Array[String]] = {
    val allNames = mutable.Set[String]()
    allNames += ne.name
    allNames ++= ne.aliases
    allNames.filter(n => n != null && n.nonEmpty)
    splitVocabEntries(allNames.toSet)
  }
  def vocab4NaEns(d:Array[NaEn]):Iterable[Iterable[String]] = {

    val res = ListBuffer[Array[String]]()
    d.foreach { e =>
      res ++= splitNames(e)
    }
    res.map(_.toVector).toList
  }

  def tm4NaEns(d:Array[NaEn], dict:Dict, tag:String): TTkMatcher = {
    val name2Ids = d.flatMap { ne =>
        val ids = Set(
          TagHelper.NaEnId(ne.neid)
        )
        val allNames = splitNames(ne).map(_.mkString(" ").toLowerCase())
        allNames.map(_ -> ids)
      }
      .groupBy(_._1)
      .mapValues(_.flatMap(_._2).toSet)
//    if (name2Ids.contains("california state university"))
//      println("ok")
    TokenMatchers.ngramExtraTags(
      name2Ids, dict, tag
    )
  }

  val UsUnivColls = ResourceHelpers.load("/naen/us_univ_coll.json", NaEn.fromJsons)
  val UsHosps = ResourceHelpers.load("/naen/us_hosp.json", NaEn.fromJsons)
  //val allVocs:Iterable[Iterable[String]] = vocab4NaEns(UsUnivColls) ++ vocab4NaEns(UsHosps)
  import TagHelper._
  private[exutil1] def tmUsUniv(dict:Dict) =
    tm4NaEns(UsUnivColls, dict, builtinTag(US_UNIV.toString))
  private[exutil1] def tmUsHosp(dict:Dict) =
    tm4NaEns(UsHosps, dict, builtinTag(US_HOSP.toString))

//  def tmsNaEn(dict:Dict) = List(
//    tmUsUniv(dict), tmUsHosp(dict)
//  )

}
