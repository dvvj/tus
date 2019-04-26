package org.ditw.sparkRuns
import org.apache.spark.sql.Row
import org.ditw.demo1.gndata.GNEnt
import org.ditw.sparkRuns.CommonUtils.findNearestAndCheck
import org.ditw.sparkRuns.csvXtr.UtilsEntCsv1.processName

object CommonCsvUtils extends Serializable {

//  def concatCols(row:Row, col:Vector[String]):String = {
//    col.map(row.getAs[String]).mkString(" ")
//  }

  def getDouble(row:Row, col:String):Double = row.getAs[String](col).toDouble

  def checkNearestGNEnt(
    ents:Iterable[GNEnt],
    row:Row,
    latCol:String,
    lonCol:String
  ):Option[GNEnt] = {
//    if (ents.size != 1) {
//      throw new RuntimeException(s"------ todo: more than one ents: $ents")
//    }
//    else {
//      val lat = getDouble(row, latCol)
//      val lon = getDouble(row, lonCol)
//      findNearestAndCheck(ents, lat->lon)
//    }
    val lat = getDouble(row, latCol)
    val lon = getDouble(row, lonCol)
    findNearestAndCheck(ents, lat->lon)

  }
}
