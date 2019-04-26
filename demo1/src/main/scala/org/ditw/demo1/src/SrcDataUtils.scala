package org.ditw.demo1.src
import org.ditw.demo1.gndata.GNEnt

object SrcDataUtils extends Serializable {

  object GNsCols extends Enumeration {
    type GNsCols = Value
    val GID,
      Name,
      AsciiName,
      AltNames,
      Latitude,
      Longitude,
      FeatureClass,
      FeatureCode,
      CountryCode,
      CountryCode2,
      Adm1,
      Adm2,
      Adm3,
      Adm4,
      Population,
      Elevation,
      Dem,
      Timezone,
      UpdateDate,
      AltNameCount = Value
  }

  import GNsCols._
  import org.ditw.demo1.gndata.GNLevel._

  private val GNsColArr = Array(
    GID, Name, AsciiName, AltNames, Latitude, Longitude,
    FeatureClass, FeatureCode, CountryCode, CountryCode2,
    Adm1, Adm2, Adm3, Adm4, Population,
    Elevation, Dem, Timezone, UpdateDate
  )
  val GNsSlimColArr = IndexedSeq(
    GID, Name, AsciiName, Latitude, Longitude,
    FeatureClass, FeatureCode, CountryCode, CountryCode2,
    Adm1, Adm2, Adm3, Adm4, Population
  )
  val GNsSlimColArrAltCount:IndexedSeq[GNsCols] = GNsSlimColArr :+ AltNameCount

  private val GNsColIdx2Enum = GNsColArr.indices
    .map(idx => idx -> GNsColArr(idx))
    .toMap
  private val GNsColEnum2Idx = GNsColArr.indices
    .map(idx => GNsColArr(idx) -> idx)
    .toMap

  def GNsCol(line:Array[String], col:GNsCols):String =
    line(GNsColEnum2Idx(col))

  private val AllIncluded = Set[String]()
  private val AdmClass = "A"
  private val PplClass = "P"
  private val featureClass2CodesMap = Map(
    AdmClass -> AllIncluded, // A: country, state, region,...
    PplClass -> AllIncluded, // P: city, village,...
    "L" -> Set( // L: parks,area, ...
      "CTRB" // business center
    )
  )

  type FeatureChecker = (String, String) => Boolean

  private val admExcludedCodes = Set(
    "ADM1H",
    "ADM2H",
    "ADM3H",
    "ADM4H",
    "ADM5H",
    "ADMDH",
    "PCLH",
    "ZN"
  )

  private val pplExcludedCodes = Set(
    "PPLCH",
    "PPLH", // historical populated place
    "PPLQ", // abandoned populated place
    "PPLW"  // destroyed populated place
  )

  private val adm0TerrCC = Set(
    "AQ", "EH", "SJ", "AS"
  )
  def isAdm0(c:String, fcode:String):Boolean = {
    (c.startsWith("PCL") && c != "PCLH") || (c == "TERR" && adm0TerrCC(fcode))
  }
  val fcAdm:FeatureChecker = (fcls:String, fcode:String) => {
    fcls == AdmClass && !admExcludedCodes.contains(fcode)
  }
  def isAdm(ent: GNEnt):Boolean = fcAdm(ent.featureClz, ent.featureCode)
  val fcPpl:FeatureChecker = (fcls:String, fcode:String) => {
    (fcls == PplClass && !pplExcludedCodes.contains(fcode)) ||
      fcode == "AIRB" // air force base
  }
  val fcAll:FeatureChecker = (fcls:String, fcode:String) => {
    fcAdm(fcls, fcode) || fcPpl(fcls, fcode) ||
      (featureClass2CodesMap.contains(fcls) && featureClass2CodesMap(fcls).contains(fcode))
  }
}
