package org.ditw.exutil1.naen
import org.apache.spark.sql.Row

case class SrcCsvMeta(
  nameCol:String,
  altNamesCol:String,
  coordCols:Option[(String, String)],
  gnCols:Vector[String],
  others:Vector[String] = SrcCsvMeta.EmptyCols
  ) {

  private val _allCols:Vector[String] = {
    val t = Vector(nameCol, altNamesCol) ++ gnCols ++ others
    if (coordCols.nonEmpty) t ++ Vector(coordCols.get._1, coordCols.get._2)
    else t
  }
  def allCols:Vector[String] = _allCols

  def gnStr(row:Row):String = {
    gnCols.map(row.getAs[String]).filter(_ != null).mkString(" ")
  }

  def name(row:Row):String = strVal(row, nameCol)
  def altNames(row:Row):String = strVal(row, altNamesCol)


  def strVal(
    row:Row,
    col:String
    //errata:Map[String, String] = SrcCsvMeta.NoErrata
  ):String = {
    row.getAs[String](col)
//    if (errata.isEmpty)
//      orig
//    else {
//      val toRepl = errata.keySet.filter(orig.contains)
//      if (toRepl.nonEmpty) {
//        var res = orig
//        toRepl.foreach(tr => res = res.replaceAll(tr, errata(tr)))
//        res
//      }
//      else orig
//    }
  }

  def strValEmptyIfNull(
              row:Row,
              col:String
              //errata:Map[String, String] = SrcCsvMeta.NoErrata
            ):String = {
    val orig = row.getAs[String](col)
    if (orig != null) orig
    else SrcCsvMeta.EmptyStr
  }

  def latCol:String = coordCols.get._1
  def lonCol:String = coordCols.get._2

  def getCoord(row:Row):(Double, Double) = {
    row.getAs[String](latCol).toDouble ->
      row.getAs[String](lonCol).toDouble
  }

  def otherKVPairs(row:Row):Vector[(String, String)] = {
    others.map(k => k -> strVal(row, k))
  }
}

object SrcCsvMeta extends Serializable {
  val EmptyCols:Vector[String] = Vector()
  val NoErrata:Map[String, String] = Map()

  val EmptyStr = ""
}
