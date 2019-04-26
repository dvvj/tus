package org.ditw.sparkRuns.csvXtr
import java.io.FileInputStream
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.ditw.exutil1.naen.{NaEn, NaEnData}

object UtilsEnt3Post {

  import EntXtrUtils._

  def main(args:Array[String]):Unit = {

    val isni = loadNaEns("/media/sf_vmshare/isni.json")
    val univs = NaEnData.UsHosps

    mergeTwoSets(univs, isni)
  }
}
