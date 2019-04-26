package org.ditw.sparkRuns
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.ditw.demo1.gndata.GNLevel._
import org.ditw.demo1.gndata._
import org.ditw.demo1.src.SrcDataUtils
import org.ditw.demo1.src.SrcDataUtils.GNsCols
import org.ditw.demo1.src.SrcDataUtils.GNsCols._

import scala.collection.mutable.ListBuffer
import org.ditw.common.GenUtils._
import org.ditw.common.SparkUtils
import org.ditw.demo1.matchers.{Adm0Gen, MatcherGen}
import org.ditw.matcher.MatchPool
import org.ditw.tknr.TknrHelpers.{TokenSplitter_CommaColon, TokenSplitter_DashSlash}
import org.ditw.tknr.{Tokenizers, Trimmers}
import org.ditw.tknr.Tokenizers.TokenizerSettings

object UtilsGNColls {
//  private val admLevel2Code = List(ADM1, ADM2, ADM3, ADM4).map(a => a -> a.toString).toMap



  import org.ditw.demo1.gndata.SrcData._

  import GNCntry._


  def main(args:Array[String]):Unit = {
    val spark = SparkUtils.sparkContextLocal()

    val gnLines = spark.textFile(
      //"/media/sf_vmshare/fp2Affs_uniq"
      "/media/sf_vmshare/gns/all"
    ).map(tabSplitter.split)
      .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    val adm0Ents = loadAdm0(gnLines)
    val brAdm0Ents = spark.broadcast(adm0Ents)

    val ccs = Set(
      US
      ,CA, GB, AU,FR,DE,ES,IT
    )
//    val adm0s = loadCountries(gnLines, ccs, brAdm0Ents)


//    ccList.foreach { cc =>
//      printlnT0(s"--- $cc")
//
//      val gnsInCC:RDD[((String,GNLevel), GNEnt)] = gnLines
//        .filter { cols =>
//          cols(countryCodeIndex) == cc && !SrcDataUtils.isPcl(cols(featureCodeIndex))
//        }
//        .map { cols =>
//          val adms = ListBuffer[String]()
//          admCols.foreach { col =>
//            if (colVal(cols, col).nonEmpty) {
//              adms += colVal(cols, col)
//            }
//          }
//          val countryCode = colVal(cols, GNsCols.CountryCode)
//          val ent = entFrom(
//            cols,
//            countryCode,
//            adms.toIndexedSeq
//          )
//
//          val admc = admCode(cols)
//          admc -> ent
//        }
//        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//      printlnT0(s"\tADM->Ent: ${gnsInCC.count}")
//
//      val t1:RDD[(Option[String], Iterable[(GNLevel, String)])] = gnsInCC
//        .groupByKey()
//        .map { p =>
//          var admEnt:Option[GNEnt] = None
//          val admCode = p._1._2.toString
//          val ents = p._2
//          ents.foreach { ent =>
//            if (ent.featureCode == admCode) {
//              if (admEnt.nonEmpty)
//                throw new IllegalArgumentException("dup adm entity?")
//              admEnt = Option(ent)
//            }
//          }
//
//          val admM1 = admMinus1Code(p._1._1)
//          val level =
//            if (admEnt.nonEmpty) admEnt.get.level
//            else p._1._2
//          admM1 -> (level, p._1._1)
//        }
//        .groupByKey()
//        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//      val admGNs = gnsInCC.groupByKey()
//        .map(p => p._1._1 -> (p._1._2 -> p._2))
//        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//      val withAdmc = t1.filter { p =>
//        //        if (p._1.isEmpty)
//        //          println("ok")
//        p._1.nonEmpty
//      }.map(p => p._1.get -> p._2)
//        .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//
//      val t3 = admGNs
//        .filter { p =>
//          p._1 != cc || p._2._1 != ADM0
//        }
//        .leftOuterJoin(withAdmc)
//        //.filter(_._2._2.isEmpty)
//        .mapValues { p =>
//        val (level, curr) = p._1
//        val (currAdm, ppls) = splitAdmAndPpl(curr, level)
//        val subAdms =
//          if (p._2.isEmpty) EmptySubAdms
//          else p._2.get.map(_._2).toIndexedSeq
//
//        val admEnt =
//          if (level == ADM0 && currAdm.isEmpty) {
//            brAdm0Ents.value.get(cc)
//          }
//          else if (level == ADM0 && currAdm.nonEmpty)
//            currAdm
//          else
//            currAdm
//        val coll = GNColls.admx(
//          level,
//          admEnt,
//          subAdms,
//          ppls.map(ppl => ppl.gnid -> ppl).toMap
//        )
//        //        if (level == ADM0)
//        //          println("ok")
//        coll
//      }
//
//      val m2 = t3.collectAsMap().toMap
//
//      assert (!m2.contains(cc))
//
//      val t = withAdmc.filter(_._1 == cc).collect()
//      val adm0GNs = admGNs.filter { p =>
//        p._1 == cc && p._2._1 == ADM0
//      }.collect()
//
//      println(s"\tADM0: ${adm0GNs.length}")
//      val gentOfAdm0 =
//        if (adm0GNs.isEmpty) EmptyMap
//        else {
//          assert(adm0GNs.length == 1)
//          adm0GNs(0)._2._2.map(ent => ent.gnid -> ent).toMap
//        }
//
//      assert (t.length == 1)
//      val admEnt = adm0Ents.get(cc)
//      val adm0 = GNColls.adm0(
//        admEnt.get, // todo
//        t(0)._2.map(_._2).toIndexedSeq,
//        gentOfAdm0,
//        m2
//      )

//      testTm(adm0,
//        List(
//          "north carolina",
//          "massachusetts"
//        )
//      )

        //m2 += cc -> adm0
//      printlnT0(s"\tAdm #: ${m2.size+1}") // + adm0
//      val allEntCount = m2.map(_._2.size).sum + adm0.size
//      printlnT0(s"\tEnt in Map: $allEntCount")

//      val strs = testStrs.getOrElse(cc, List())
//      strs.foreach { ts =>
//        val parts = ts.split(",").map(_.trim).filter(_.nonEmpty)
//        val res = adm0.byName(parts(0), cc + "_" + parts(1))
//        printlnT0(res)
//      }
//    }

    spark.stop()
  }



  val testStrs = Map(
    "US" -> List[String](
      "Washington, DC",
      "Washington, MN",
      "Minneapolis, MN",
      "Research Triangle Park, NC"
    ),
    "CA" -> List[String](
      "Montréal, Québec"
    )
  )
  //private val EmptyPpls = IndexedSeq[GNEnt]()

}
