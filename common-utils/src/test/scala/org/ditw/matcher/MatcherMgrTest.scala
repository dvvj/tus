package org.ditw.matcher
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class MatcherMgrTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  import CompMatchers._
  import CompMatcherNs._

  private val tag2Find1 = "tag2Find1"
  private val tag2Find2 = "tag2Find2"
  private val tag2Find3 = "tag2Find3"
  private val tagByTag1 = "tagByTag1"
  private val tagByTag2 = "tagByTag2"
  private val tagOr1 = "tagOr1"
  private val byTag1 = byTags(Set(tag2Find1, tag2Find2), tagByTag1)
  private val byTag2 = byTags(Set(tag2Find1, tag2Find3), tagByTag2)
  private val or1 = or(Set(byTag1, byTag2), tagOr1)

  private val cmDepMapTestData = Table(
    ( "cms", "depMap" ),
    (
      List(byTag1, byTag2, or1),
      Map(
        tag2Find1 -> Set(tagByTag1, tagByTag2),
        tag2Find2 -> Set(tagByTag1),
        tag2Find3 -> Set(tagByTag2),
        tagByTag1 -> Set(tagOr1),
        tagByTag2 -> Set(tagOr1)
      )
    )
  )

  "depTagMap tests" should "pass" in {
    forAll(cmDepMapTestData) { (cms, depMap) =>
      val mmgr = new MatcherMgr(List(), List(), cms, List())
      mmgr.cmDepMap shouldBe depMap
    }
  }

  private val tagErrorTestData = Table(
    ("cms", "errorMsg"),
    (
      List(
        byTag1, byTag2, or1,
        byTag("tmpTag1")
      ),
      "Empty Tag Matcher found!"
    ),
    (
      List(
        byTag1, byTag1, byTag2, byTag2, or1,
        byTag("tmpTag1")
      ),
      "Empty Tag Matcher found!"
    ),
    (
      List(byTag1, byTag1, byTag2, byTag2, or1),
      "Duplicate tag(s) found: [tagByTag1,tagByTag2]"
    ),
    (
      List(byTag1, byTag1, byTag1, byTag2, or1),
      "Duplicate tag(s) found: [tagByTag1]"
    ),
    (
      List(byTag1, byTag1, or1),
      "Duplicate tag(s) found: [tagByTag1]"
    )
  )

  "tag errors" should "be detected" in {
    forAll(tagErrorTestData) { (cms, errorMsg) =>
      val caught = intercept[IllegalArgumentException] {
        val mmgr = new MatcherMgr(List(), List(), cms, List())
      }
      caught.getMessage shouldBe errorMsg
    }

  }
}
