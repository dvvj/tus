package org.ditw.pmxml.model

import com.lucidchart.open.xtract.{ XmlReader, __ }
import Constants.XmlTags._
import cats.syntax.all._

import DateCommon._
case class DateCommon(
                     year:Int,
                     month:Option[Int],
                     day:Option[Int],
                     medlineDate:Option[String]
                     ) {
//  val _year:Int = {
//    if (year.nonEmpty) year.get
//    else {
//      if (medlineDate.nonEmpty) {
//        val m = d4reg.findFirstIn(medlineDate.get)
//        if (m.nonEmpty) {
//          m.get.toInt
//        }
//        else {
//          throw new IllegalArgumentException(s"No year info in medlinedata [$medlineDate]")
//        }
//      }
//      else {
//        throw new IllegalArgumentException(s"both year and medlinedate are empty")
//      }
//    }
//  }
//  val _month:Option[Int] = {
//
//    if (month.isEmpty)
//      None
//    else {
//      val monStr = month.get
//      val monVal = if (monStr.forall(_.isDigit)) {
//        monStr.toInt
//      }
//      else {
//        monthMap(monStr)
//      }
//      Option(monVal)
//    }
//  }
//
//  def getMonth:Option[Int] = _month

}

object DateCommon {

  private def getYear(year:Option[Int], medlineDate:Option[String]):Int = {
    if (year.nonEmpty) year.get
    else {
      if (medlineDate.nonEmpty) {
        val m = d4reg.findFirstIn(medlineDate.get)
        if (m.nonEmpty) {
          m.get.toInt
        }
        else {
          throw new IllegalArgumentException(s"No year info in medlinedata [$medlineDate]")
        }
      }
      else {
        throw new IllegalArgumentException(s"both year and medlinedate are empty")
      }
    }
  }

  private def getMonth(month:Option[String]):Option[Int] = {
    if (month.isEmpty)
      None
    else {
      val monStr = month.get
      val monVal = if (monStr.forall(_.isDigit)) {
        monStr.toInt
      }
      else {
        monthMap(monStr)
      }
      Option(monVal)
    }
  }

  private val d4reg = "\\d{4}".r

  private val monthMap:Map[String, Int] = Map(
    List("Jan") -> 1,
    List("Feb") -> 2,
    List("Mar") -> 3,
    List("Apr") -> 4,
    List("May") -> 5,
    List("Jun") -> 6,
    List("Jul") -> 7,
    List("Aug") -> 8,
    List("Sept", "Sep") -> 9,
    List("Oct") -> 10,
    List("Nov") -> 11,
    List("Dec") -> 12
  ).flatMap(kv => kv._1.map(_ -> kv._2))

  def fromOrig(dateOrig:DateOrig):DateCommon = {
    DateCommon(
      getYear(dateOrig.year, dateOrig.medlineDate),
      getMonth(dateOrig.month),
      dateOrig.day,
      dateOrig.medlineDate
    )
  }
//  implicit val reader:XmlReader[DateCommon] = (
//    (__ \ Year).read[Int].optional,
//    (__ \ Month).read[String].optional,
//    (__ \ Day).read[Int].optional,
//    (__ \ MedlineDate).read[String].optional
//  ).mapN(apply)
}
