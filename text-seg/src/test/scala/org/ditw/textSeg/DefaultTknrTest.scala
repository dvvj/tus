package org.ditw.textSeg
import org.ditw.tknr.TknrResult
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class DefaultTknrTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import org.ditw.tknr.TknrHelpers._
  import TestHelpers._

  private val tknrTestData = Table(
    ("inStr", "expRes"),
    testDataTuple(
      "Universität Göttingen, F.R.G.",
      IndexedSeq(
        noPfxSfx("Universität"),
        noPfx("Göttingen", ","),
        noPfx("F.R.G", ".")
      )
    ),
    testDataTuple(
      "Email: D.Stuening@zfmk.de",
      IndexedSeq(
        noPfx("Email", ":"),
        noPfxSfx("D.Stuening@zfmk.de")
      )
    ),
    testDataTuple(
      "Email: D.Stuening@zfmk.de.",
      IndexedSeq(
        noPfx("Email", ":"),
        noPfx("D.Stuening@zfmk.de", ".")
      )
    ),
    testDataTuple(
      "⊥Université d'Orléans and",
      IndexedSeq(
        noSfx("Université", "⊥"),
        noPfxSfx("d'Orléans"),
        noPfxSfx("and")
      )
    ),
    testDataTuple(
      "┼Insight Centre ╫School of",
      IndexedSeq(
        noSfx("Insight", "┼"),
        noPfxSfx("Centre"),
        noSfx("School", "╫"),
        noPfxSfx("of")
      )
    ),
    testDataTuple(
      "⑊ Mersin University.",
      IndexedSeq(
        noSfx("", "⑊"),
        noPfxSfx("Mersin"),
        noPfx("University", ".")
      )
    ),
    testDataTuple(
      "\uD835\uDD434 Collaboration &",
      IndexedSeq(
        noSfx("4", "\uD835\uDD43"),
        noPfxSfx("Collaboration"),
        noPfxSfx("&")
      )
    ),
    testDataTuple(
      "●●Physicians Committee",
      IndexedSeq(
        noSfx("Physicians", "●●"),
        noPfxSfx("Committee")
      )
    ),
    testDataTuple(
      "☆DAC SRL,",
      IndexedSeq(
        noSfx("DAC", "☆"),
        commaSfx("SRL")
      )
    ),
    testDataTuple(
      "⬠Instituto Processi Chimico-Fisici, CNR-IPCF,",
      IndexedSeq(
        noSfx("Instituto", "⬠"),
        noPfxSfx("Processi"),
        commaSfx("Chimico-Fisici"),
        commaSfx("CNR-IPCF")
      )
    ),
    testDataTuple(
      "★CH2M HILL, San",
      IndexedSeq(
        noSfx("CH2M", "★"),
        commaSfx("HILL"),
        noPfxSfx("San")
      )
    )
  )

  "Default Tokenizer tests" should "pass" in {
    forAll(tknrTestData) { (inStr, expRes) =>
      val res = testTokenizer.run(inStr, _Dict)

      resEqual(res, expRes) shouldBe true

    }
  }

}
