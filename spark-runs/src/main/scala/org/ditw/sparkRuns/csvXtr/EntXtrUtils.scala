package org.ditw.sparkRuns.csvXtr
import java.io.FileInputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.ditw.common.{InputHelpers, ResourceHelpers}
import org.ditw.exutil1.naen.{NaEn, SrcCsvMeta}
import org.ditw.sparkRuns.CommonUtils.csvRead
import org.ditw.sparkRuns.alias.UfdEntitySiteAlias
import org.ditw.sparkRuns.csvXtr.IsniSchema.csvMeta
import org.ditw.sparkRuns.pmXtr.AliasHelper
import org.ditw.tknr.TknrHelpers

import scala.collection.mutable.ListBuffer

object EntXtrUtils extends Serializable {
  def taggedErrorMsg(tag:Int, msg:String):Option[String] = {
    Option(s"{$tag} $msg")
  }

  type RawRowProcessRes[R] = (String, Option[R], Option[String])
  def process[T](df: DataFrame,
                 rowFunc:Row => RawRowProcessRes[T],
                 entFunc:(RawRowProcessRes[T], Long) => NaEn
                ):(Array[NaEn], Array[String]) = {
    val t = df.rdd.map(rowFunc)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val ents = t.filter(_._2.nonEmpty)
      .sortBy(_._1)
      .zipWithIndex()
      .map(p => entFunc(p._1, p._2))
      .collect()
    println(s"Ent   #: ${ents.length}")
    val errors = t.filter(_._2.isEmpty)
      .sortBy(_._1)
      .map { p =>
        val (ri, _, errMsg) = p
        s"$ri: ${errMsg.get}"
      }.collect()
    println(s"Error #: ${errors.length}")
    ents -> errors
  }

  def loadNaEns(f:String):Array[NaEn] = {
    val is = new FileInputStream(f)
    val srcStr = IOUtils.toString(is, StandardCharsets.UTF_8)
    is.close()
    NaEn.fromJsons(srcStr)
  }

  private val EmptyAliases = List[String]()
  private val substituteMap = Map(
    "university" -> List("univ", "univ.")
  )

  def loadUfdNaEns(f:String):Array[NaEn] = {
//    val ufdAliases = ResourceHelpers.load("/ufd_entity/aliases.json", UfdEntityAlias.load)
//      .map(als => als.neid -> als.aliases).toMap

    val siteAlias = ResourceHelpers.load("/ufd_entity/sites.json", UfdEntitySiteAlias.load)
      .flatMap { sn =>
        sn.sites.flatMap(p => sn.names.map(p.isni -> _.toLowerCase()))
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2))

    val isniAliases = ResourceHelpers.load("/isni_aliases.json", IsniEnAlias.load)
      .map(als => als.isni -> als.aliases)
      .groupBy(_._1)
      .mapValues { p =>
        if (p.length > 1)
          throw new RuntimeException(s"duplicates: ${p.mkString(",")}")
        p.head._2
      }

    val isniBlocked = ResourceHelpers.load("/isni_blocked.json", IsniBlocked.load)
      .map(bl => bl.isni -> bl).toMap

    EntXtrUtils.loadNaEns(f)
      .flatMap { en =>
        //val isni = en.attr("isni")
        val _isni = en.attr("isni")
        if (_isni.isEmpty) {
          Option(en)
        }
        else if (isniBlocked.contains(_isni.get))
          None
        else {
          val isni = _isni.get
          var exAliases =
            if (isniAliases.contains(isni))
            // todo: val merged = en.aliases ++ isniAliases(isni)
              isniAliases(isni)
            else EmptyAliases

          val lowerName = en.name.toLowerCase()
          if (lowerName.startsWith(theStart))
            exAliases ::= en.name.substring(theStart.length)

          val alt1 = AliasHelper.univSchoolCollegeAlias(lowerName)
          if (alt1.nonEmpty)
            exAliases ::= alt1.get

          if (siteAlias.contains(isni))
            exAliases ++= siteAlias(isni)

//          if (exAliases.contains("sanford-burnham medical research institute"))
//            println("ok")

          val allNamesSofar = (lowerName :: exAliases).map(_.toLowerCase())

          substituteMap.foreach { p =>
            val (toFind, repl) = p
            allNamesSofar.foreach { an =>
              if (an.contains(toFind)) {
                val als = repl.map(r => an.replace(toFind, r))
                exAliases ++= als
              }
            }
          }
          //        if (isniEntSites.contains(lowerName))
          //        // todo: val merged = en.aliases ++ isniAliases(isni)
          //          exAliases ++= isniEntSites(lowerName)

          val aliasesFix = InputHelpers.fixManualPhrases(exAliases)

          Option(en.copy(aliases = aliasesFix.toList))
        }

      }
  }

//  private def checkManualInputValidity(aliases:Iterable[String]):Unit = {
//    val containsInvalid = aliases.exists { al =>
//      val dashIndices = al.indices.filter(idx => al(idx) == '-' || al(idx) == '-')
//
//    }
//    if ()
//  }
  private val theStart = "the "


  def loadIsniNaEns(f:String):Array[NaEn] = {
    val isniAliases = ResourceHelpers.load("/isni_aliases.json", IsniEnAlias.load)
      .map(als => als.isni -> als.aliases).toMap

    val isniBlocked = ResourceHelpers.load("/isni_blocked.json", IsniBlocked.load)
      .map(bl => bl.isni -> bl).toMap

    val isniEntSites = ResourceHelpers.load("/isni_ent_sites.json", IsniSiteNames.load)
      .flatMap(sn => sn.siteNames.map(_.toLowerCase() -> sn.names)).toMap

    EntXtrUtils.loadNaEns(f)
      .flatMap { en =>
        val _isni = en.attr("isni")
        if (_isni.isEmpty) {
          Option(en)
        }
        else if (isniBlocked.contains(_isni.get))
          None
        else {
          val isni = _isni.get
          var exAliases =
            if (isniAliases.contains(isni))
            // todo: val merged = en.aliases ++ isniAliases(isni)
              isniAliases(isni)
            else EmptyAliases

          val lowerName = en.name.toLowerCase()
          if (lowerName.startsWith(theStart))
            exAliases ::= en.name.substring(theStart.length)

          val alt1 = AliasHelper.univSchoolCollegeAlias(lowerName)
          if (alt1.nonEmpty)
            exAliases ::= alt1.get

          if (isniEntSites.contains(lowerName))
          // todo: val merged = en.aliases ++ isniAliases(isni)
            exAliases ++= isniEntSites(lowerName)

          Option(en.copy(aliases = exAliases))
        }
      }
  }

  def mergeTwoSets(collSet:Array[NaEn], unitSet:Array[NaEn]):Unit = {
    collSet.foreach { csEnt =>
      val csNameLower = csEnt.name.toLowerCase()
      val contained = ListBuffer[NaEn]()
      unitSet.foreach { usEnt =>
        if (usEnt.gnid == csEnt.gnid &&
          usEnt.name.toLowerCase().contains(csNameLower)) {
          contained += usEnt
        }
      }
      if (contained.nonEmpty) {
        println(s"${csEnt.name}: $csEnt")
        println(contained.mkString("\t", "\n\t", ""))
      }
    }
  }

//  def errorRes[T](rowInfo:String, errMsg:String):(String, Option[T], Option[String]) = {
//    (
//      rowInfo, None, Option(errMsg)
//    )
//  }

  def loadIsni(
    spSess: SparkSession,
    csvPath:String
  ):DataFrame = {

    import spSess.implicits._

    val orig = csvRead(
      spSess, csvPath, csvMeta
    )

    orig
//    val errata = ResourceHelpers.load("isni_errata.json", IsniErrata.load)
//
//
//    var res = orig
//    errata.foreach { e =>
//      res.sqlContext.sql("update ")
//    }
  }
}
