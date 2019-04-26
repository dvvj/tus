import org.ditw.demo1.TestData.testGNSvc
import org.ditw.demo1.gndata.GNCntry._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class CollGetByNameTests extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private val testData = Table(
    ("cntry", "name", "expResults"),
    (
      GB, "Hythe", Set(2646316L, 2646317L)
    )
  )

  "byName tests" should "pass" in {
    forAll(testData) { (cntry, name, expResults) =>
      val ents = testGNSvc.entsByName(cntry, name)
        .map(_.gnid).toSet

      ents shouldBe expResults

    }
  }

}
