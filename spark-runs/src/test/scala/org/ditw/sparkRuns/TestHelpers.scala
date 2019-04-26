package org.ditw.sparkRuns
import org.apache.spark.SparkContext
import org.ditw.common.ResourceHelpers
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.GNSvc
import org.ditw.exutil1.naen.NaEnData.NaEnCat.{ISNI, UFD, US_HOSP, US_UNIV}
import org.ditw.exutil1.naen.TagHelper
import org.ditw.exutil1.naen.TagHelper.builtinTag
import org.ditw.sparkRuns.CommonUtils.GNMmgr
import org.ditw.sparkRuns.csvXtr.EntXtrUtils

object TestHelpers extends Serializable {

  import org.ditw.demo1.gndata.GNCntry._
  private val _gnLines = ResourceHelpers.loadStrs("/gns/test_gns.csv")
  private[sparkRuns] val _ccs = Set(US, PR, CA)
  private val _ccms = Set(PR)

  import CommonUtils._
  import org.ditw.exutil1.naen.NaEnData._
  def testGNMmgr(
                 spark:SparkContext
                ):GNMmgr = {
    val gnLines = spark.parallelize(_gnLines)
    _loadGNMmgr(_ccs, _ccms, spark, gnLines,
//      Map(
//        builtinTag(US_UNIV.toString) -> UsUnivColls,
//        builtinTag(US_HOSP.toString) -> UsHosps
//      )
      Map(
        TagHelper.builtinTag(UFD.toString) -> EntXtrUtils.loadUfdNaEns("/media/sf_vmshare/ufd-isni.json")
      ),
      true
    )
  }
}
