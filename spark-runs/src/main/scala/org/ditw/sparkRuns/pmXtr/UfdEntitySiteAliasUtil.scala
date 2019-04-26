package org.ditw.sparkRuns.pmXtr
import org.ditw.common.ResourceHelpers
import org.ditw.exutil1.naen.NaEn
import org.ditw.sparkRuns.CommonUtils
import org.ditw.sparkRuns.alias.{UfdEntitySite, UfdEntitySiteAlias}
import org.ditw.sparkRuns.csvXtr.EntXtrUtils

object UfdEntitySiteAliasUtil {

//  private val regexs = List(
//    "(?i)(.*university.*\\s+(at|in|-)\\s+\\w+(\\s+\\w+){0,2})".r
//    //,".*(?i)university(\\s+\\w+){1,3}".r
//  )
  private val blacklistWords = List(
    "department",
    "school",
    "college",
    "division",
    "bookstore",
    "book store",
    "store",
    "library",
    "libraries",
    "hospital",
    "press",
    "institute",
    "center",
    "centre",
    "unit",
    "foundation",
    "health",
    "association",
    "inc",
    "studies",
    "programs",
    "program",
    "online",
    "lirary",
    "laboratory",
    "lab",
    "stores",
    "faculty"
  )
  val univstr = "university"

//  val knownNames = Set(
//    "University of North Carolina",
//    "University of Wisconsin",
//    "American InterContinental University",
//    "University of Missouri",
//    "University of California",
//    "California State University",
//    "University at Buffalo",
//    "Loyola University",
//    "Rutgers University",
//    "State University of New York",
//    "University of Texas Medical Branch",
//    "University of Georgia",
//    "University of Hawaii",
//    "Mayo Clinic",
//    "Janssen Research and Development"
//  )

  val knownNames = ResourceHelpers.loadStrs("/alias/curr_known_names.txt")
    .map(_.toLowerCase())
    .sortBy(_.length)(Ordering[Int].reverse)

  def main(args:Array[String]):Unit = {
    val ufdEns = EntXtrUtils.loadNaEns("/media/sf_vmshare/ufd-isni.json")

    val ufdNameMap = ufdEns.map(en => en.name -> en)
      .groupBy(_._1)
      .mapValues(_.map(_._2).toList)

    val nonUniq = ufdNameMap.filter(_._2.length != 1)

    println(
      nonUniq.mkString("\n")
    )

    val uniqMap:Map[String, NaEn] = ufdNameMap.filter(_._2.length == 1)
        .map(p => p._1.toLowerCase() -> p._2.head)

    println("=====================================================")
    val sa = ufdEns
      .flatMap { en =>
        val lowerName = en.name.toLowerCase()
        val lowerNameParts = lowerName.split("\\s+").filter(!_.isEmpty).toSet
        val knownName = knownNames.filter(lowerName.startsWith).headOption
        if (knownName.nonEmpty &&
          !blacklistWords.exists(lowerNameParts.contains)) {
//          if (knownName.length > 1)
//            println(s"More than 1 known names!? (${knownName.mkString(",")}")
          Option(knownName.get.toLowerCase() -> en)
        }
        else None
//        val lowerName = en.name.toLowerCase()
//        val lowerNameParts = lowerName.split("\\s+").filter(!_.isEmpty).toSet
//        if (regexs.exists(_.pattern.matcher(lowerName).matches()) &&
//          !blacklistWords.exists(lowerNameParts.contains)
//        ) {
//          val univIdx = lowerName.indexOf(univstr)
//          val univName = lowerName.substring(0, univIdx + univstr.length).trim
//          if (uniqMap.contains(univName)) {
//            Option(univName -> en)
//          }
//          else {
//            println(s"[$univName] not found!")
//            None
//          }
//        }
//        else None
      }
      .groupBy(_._1)
      .map { p =>
        val sites = p._2.map { p =>
          val isni = p._2.attr("isni").get
          val n = p._2.name
          isni -> n
          UfdEntitySite(isni, n)
        }.sortBy(_.n)
//        val sitesTr = sites.mkString("\t", "\n\t", "")
//        println(s"${p._1}\n$sitesTr")
        UfdEntitySiteAlias(Array(p._1), sites)
      }
      .toArray
      .sortBy(_.names.head)

    import org.ditw.common.GenUtils._
    val res = UfdEntitySiteAlias.toJson(sa)

    writeStr("/media/sf_vmshare/curr_ufd_site_alias.json", res)
  }
}
