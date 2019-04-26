package org.ditw.demo1.matchers
import org.ditw.demo1.TestData.{testDict, testGNSvc}
import org.ditw.demo1.matchers.MatcherHelper.{mmgr, xtrMgr}
import org.ditw.matcher.MatchPool
import org.ditw.tknr.TknrHelpers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class ExtractionTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("inStr", "expGNIds"),
    (
      "Massachusetts Institute of Technology, 77 Massachusetts Avenue, Massachusetts 02139, Cambridge, USA.",
      Set(4931972L)
    ),
    (
      "SAN JUAN PR",
      Set(4568127L)
    ),
    (
      "Massachusetts Institute of Technology, 77 Massachusetts Avenue, Cambridge, Massachusetts 02139, USA.",
      Set(4931972L)
    ),
    (
      "Harvard Medical School, Boston, MA 02115, USA.",
      Set(4930956L)
    ),
    (
      "MA 02115, USA",
      Set(4930956L)
    ),
    (
      "CF10 3NB, UK",
      Set(2653822L)
    ),
    (
      "Cardinal Stefan Wyszynski University in Warsaw",
      Set[Long]()
    ),
    (
      "Miyata Eye Hospital Miyakonojo-shi Miyazaki-ken Japan",
      Set(1856774L)
    ),
    (
      "Yufu-City, Oita 879-5593, Japan",
      Set(8739980L)
    ),
    (
      "Iizuka Fukuoka 820-8505 Japan",
      Set(1861835L)
    ),
    (
      "University Park, PA, 16802, USA.",
      Set(5216774L)
    ),
    (
      "CLINTON TWP MI",
      Set(4989133L)
    ),
    (
      "Boston, Massaschusetts,USA.",
      Set(4930956L)
    ),
//    (
//      "Harvard School of Public Health Massaschussets, Cambridge, USA.",
//      Set(4931972L)
//    ),
    (
      "Cambridge, Massachussets, USA.",
      Set(4931972L)
    ),
    (
      "Portland, Ore. 97239, USA.",
      Set(5746545L)
    ),
//    (
//      "New York, USA.",
//      Set(5128581L)
//    ),
    (
      "The Medical Banking Project, Franklin, Tenn, USA.",
      Set(4623560L)
    ),
    (
      "Cambridge, MA 02138, USA.",
      Set(4931972L)
    ),
    (
      "Boston, MA, USA.",
      Set(4930956L)
    ),
    (
      "Boston, Massachusetts, USA.",
      Set(4930956L)
    ),
    (
      "Bunkyo-Ku, Tokyo 113-0033, Japan",
      Set(7475133L)
    ),
    (
      "Bunkyō, Tokyo 113-0033, Japan",
      Set(7475133L)
    ),
    (
      "Minato-ku, Tokyo",
      Set(1857091L)
    ),
    (
      "Shinjuku, Tokyo",
      Set(1852083L)
    ),
    (
      "Shinjuku, Tokyo, Japan.",
      Set(1852083L)
    ),
    (
      "Shinjuku-ku, Tokyo 160-8582, Japan.",
      Set(1852083L)
    ),
    (
      "Yufu, Oita 879-5593, Japan",
      Set(11612342L)
    ),
//    (
//      "Oita, Japan",
//      Set(1854487L)
//    ),
    (
      "Onga, Fukuoka Japan.",
      Set(7407422L)
    ),
    (
      "Chuo-ku, Fukuoka 810-0051, Japan.",
      Set(7407400L)
    ),
//    (
//      "Kanda Chiyodaku Japan",
//      Set(11749713L)
//    ),
    (
      "Yufu-shi, Oita 879-5593, Japan",
      Set(8739980L)
    ),
    (
      "Beppu, Oita, 874-0838, Japan",
      Set(1864750L)
    ),
    (
      "Imizu City, Toyama-ken 937-8585, Japan",
      Set(6822125L)
    ),
    (
      "Imizu City, Toyama 937-8585, Japan",
      Set(6822125L)
    ),
    (
      "Yufu, Oita 879-5593, Japan",
      Set(11612342L)
    ),
    (
      "University of Southern California, Los Angeles, California, USA",
      Set(5368361L)
    ),
//    (
//      "Komatsu Japan",
//      Set(1858910L)  // before disambiguation
//    ),
    (
      "Washington University School of Medicine, St. Louis, MO, USA.",
      Set(4407066L)  // before disambiguation
    ),
    (
      "Washington University School of Medicine, St Louis, MO, USA.",
      Set(4407066L)  // before disambiguation
    )
  )

  "extraction tests" should "pass" in {
    forAll(testData) { (inStr, expGNIds) =>
      val mp = MatchPool.fromStr(inStr, MatcherHelper.testTokenizer, testDict)
      mmgr.run(mp)
      val rng2Ents = testGNSvc.extrEnts(xtrMgr, mp)
      val res = rng2Ents.flatMap(_._2.map(_.gnid)).toSet
      res shouldBe expGNIds

    }
  }

}
