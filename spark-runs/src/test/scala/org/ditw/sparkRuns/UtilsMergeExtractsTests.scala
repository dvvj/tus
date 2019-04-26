package org.ditw.sparkRuns
import org.ditw.common.SparkUtils
import org.ditw.textSeg.output.{AffGN, SegGN}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class UtilsMergeExtractsTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("existing", "toAdd", "expResult"),
    (
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1"))))
      ),
      Vector(
        SegGN("univ2", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1")))),
        SegGN("univ2", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10001-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1", "10001-1"))))
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10001-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(AffGN(1L, "gn1", Vector("10000-1")))),
        SegGN("univ2", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(2L, "gn2", Vector("10002-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        ),
        SegGN("univ2", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(AffGN(2L, "gn2", Vector("10001-1"))))
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10003-1")),
          AffGN(2L, "gn2", Vector("10001-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1", "10003-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10003-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1", "10003-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        ),
        SegGN("univ2", Vector(
          AffGN(1L, "gn1", Vector("20000-1")),
          AffGN(2L, "gn2", Vector("20002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        ),
        SegGN("univ2", Vector(
          AffGN(1L, "gn1", Vector("20000-1")),
          AffGN(2L, "gn2", Vector("20002-1")))
        )
      )
    ),
    (
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10002-1")))
        ),
        SegGN("univ2", Vector(
          AffGN(1L, "gn1", Vector("20000-1")),
          AffGN(2L, "gn2", Vector("20002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        ),
        SegGN("univ3", Vector(
          AffGN(1L, "gn1", Vector("30000-1")),
          AffGN(2L, "gn2", Vector("30002-1")))
        )
      ),
      Vector(
        SegGN("univ1", Vector(
          AffGN(1L, "gn1", Vector("10000-1")),
          AffGN(2L, "gn2", Vector("10001-1", "10002-1")),
          AffGN(3L, "gn3", Vector("10003-1")))
        ),
        SegGN("univ2", Vector(
          AffGN(1L, "gn1", Vector("20000-1")),
          AffGN(2L, "gn2", Vector("20002-1")))
        ),
        SegGN("univ3", Vector(
          AffGN(1L, "gn1", Vector("30000-1")),
          AffGN(2L, "gn2", Vector("30002-1")))
        )
      )
    )
  )

  "Merge Extracts tests" should "pass" in {
    val spark = SparkUtils.sparkContextLocal()

    forAll(testData) { (existing, toAdd, expResult) =>
      val exSgns = spark.parallelize(existing).map(sgn => sgn.name -> sgn)
      val taSgns = spark.parallelize(toAdd).map(sgn => sgn.name -> sgn)
      val merged = UtilsMergeExtracts.doMerge(exSgns, taSgns)
        .sortBy(_.name)
        .collect()
      merged shouldBe expResult
    }

    spark.stop()
  }

}
