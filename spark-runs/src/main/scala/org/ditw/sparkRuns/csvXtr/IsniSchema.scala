package org.ditw.sparkRuns.csvXtr
import org.apache.spark.sql.Row
import org.ditw.exutil1.naen.SrcCsvMeta

object IsniSchema extends Serializable {
  private[csvXtr] val headers =
    "isni,name,alt_names,locality,admin_area_level_1_short,post_code,country_code,urls"
  private[csvXtr] val ColISNI = "isni"
  private[csvXtr] val ColName = "name"
  private[csvXtr] val ColAltNames = "alt_names"
  private[csvXtr] val ColCity = "locality"
  private[csvXtr] val ColAdm1 = "admin_area_level_1_short"
  private[csvXtr] val ColPostal = "post_code"
  private[csvXtr] val ColCountryCode = "country_code"

  private[csvXtr] val csvMeta = SrcCsvMeta(
    ColName,
    ColAltNames,
    None,
    Vector(ColCity, ColAdm1, ColCountryCode),
    Vector(ColISNI, ColPostal)
  )

  private[csvXtr] def rowInfo(row:Row):String = {
    val isni = row.getAs[String](ColISNI)
    val name = row.getAs[String](ColName)
    s"$name($isni)"
  }



}
