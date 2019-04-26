package org.ditw.common
import java.io.{FileInputStream, FileOutputStream}
import java.nio.charset.StandardCharsets
import java.text.Normalizer

import org.apache.commons.io.IOUtils
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object GenUtils extends Serializable {

  def printlnT(msg:String, indent:Int = 0):Unit = {
    val n = DateTime.now().toString("hh:mm:ss.SSS")
    val indStr = (0 until indent).map(_ => "\t").mkString
    println(s"$indStr[$n] $msg")
  }

  //  private def _printlnT_X(fmt:String, ind:Int, param:Any*):Unit = printlnT(String.format(fmt, param), ind)
  //  def printlnT01(fmt:String, param:Any):Unit = _printlnT_X(fmt, 0, param)
  //  def printlnT02(fmt:String, param1:Any, param2:Any):Unit = _printlnT_X(fmt, 0, param1, param2)
  def printlnT0(a:Any):Unit = printlnT(a.toString, 0)
  def printlnT0(msg:String):Unit = printlnT(msg, 0)
  def printlnT1(msg:String):Unit = printlnT(msg, 1)
  def printlnT2(msg:String):Unit = printlnT(msg, 2)

  private val replRegex = "\\p{M}".r
  def normalize(n:String):String = {
    val nr = Normalizer.normalize(n, Normalizer.Form.NFD)
    if (nr == n) n
    else {
      replRegex.replaceAllIn(nr, "")
    }
  }
  def removeAccents(str:String):String = replRegex.replaceAllIn(str, "")

  def splitPreserve(in:String, sp:Char):Vector[String] = {
    val res = new ListBuffer[String]()

    var hasMore = true
    var start = 0
    while (hasMore && start < in.length) {
      val n = in.indexOf(sp, start)
      if (n >= 0) {
        val p = in.substring(start, n)
        if (p.nonEmpty) {
          res += p
        }
        res += in.substring(n, n+1)
        start = n+1
      }
      else {
        hasMore = false
        res += in.substring(start)
      }
    }
    res.toVector
  }


  def writeJson[T](path:String, objs:Array[T], conv:Array[T] => String):Unit = {
    val out = new FileOutputStream(path)
    IOUtils.write(conv(objs), out, StandardCharsets.UTF_8)
    out.close()
  }

  def readJson[T](path:String, conv:String => Array[T]):Array[T] = {
    val strm = new FileInputStream(path)
    val j = IOUtils.toString(strm, StandardCharsets.UTF_8)
    strm.close()
    conv(j)
  }
  def writeStr(path:String, strs:Array[String]):Unit = {
    val out = new FileOutputStream(path)
    IOUtils.write(strs.mkString("\n"), out, StandardCharsets.UTF_8)
    out.close()
  }
  def writeStr(path:String, str:String):Unit = {
    val out = new FileOutputStream(path)
    IOUtils.write(str, out, StandardCharsets.UTF_8)
    out.close()
  }
}
