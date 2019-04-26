package org.ditw.exutil1.naenRepo
import org.ditw.exutil1.naen.NaEn
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class NaEnRepoTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  type EntInfo = (Long, String)
  private val catBits = 36
  private val catMask = -1L << catBits
  private def cat(c:Int):Long = c.toLong << catBits
  private val NA_gnid = -1L
  private val TestNewCatBaseId:Long = 0x10000L
  private def entId(c:Int, eid:Int):Long = cat(c) | (TestNewCatBaseId + eid)
  private def ent(c:Int, eid:Int):NaEn = NaEn(
    entId(c, eid),
    s"ent-$c-$eid",
    List(),
    NA_gnid
  )
  private def entRepo(ents:Iterable[NaEn]): NaEnRepo[EntInfo] = new NaEnRepo[(Long, String)](
    ents,
    "test repo",
    id => id & catMask,
    ei => ei._1 & catMask,
    (id, ei) => NaEn(id, ei._2, List(), NA_gnid),
    TestNewCatBaseId
  )

  private def newEnt(c:Int, count:Int):Iterable[EntInfo] = {
    (0 until count).map { idx =>
      (cat(c), s"new-ent-$c-$idx")
    }
  }

  private val addTestData = Table(
    ( "testRepo", "newEnts", "expCat2Count", "expCatMaxMap" ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1),
        ent(2, 0)
      )),
      newEnt(1, 2) ++
        newEnt(2, 1) ++
        newEnt(3, 2),
      Map(
        cat(1) -> 4,
        cat(2) -> 2,
        cat(3) -> 2
      ),
      Map(
        cat(1) -> (cat(1) + TestNewCatBaseId + 3),
        cat(2) -> (cat(2) + TestNewCatBaseId + 1),
        cat(3) -> (cat(3) + TestNewCatBaseId + 1)
      )
    )
  )

  "new entity tests" should "pass" in {
    forAll(addTestData) { (testRepo, newEnts, expCat2Count, expCatMaxMap) =>
      newEnts.foreach(testRepo.add)
      testRepo.catCount shouldBe expCat2Count
      testRepo.catMaxMap shouldBe expCatMaxMap
    }
  }

  private val diffTestData = Table(
    ( "repo1", "repo2", "expDiff" ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1),
        ent(2, 0)
      )),
      entRepo(Iterable(
        ent(1, 0),
        ent(2, 0)
      )),
      (
        Set(entId(1, 1)) -> Set[Long]()
      )
    ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1)
      )),
      entRepo(Iterable(
        ent(1, 0),
        ent(2, 0)
      )),
      (
        Set(entId(1, 1)) -> Set[Long](entId(2, 0))
        )
    ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1)
      )),
      entRepo(Iterable(
        ent(2, 0)
      )),
      (
        Set(
          entId(1, 1),
          entId(1, 0)
        ) -> Set[Long](entId(2, 0))
      )
    ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1)
      )),
      entRepo(Iterable(
        ent(2, 0),
        ent(1, 0),
        ent(1, 1)
      )),
      (
        Set[Long]() -> Set[Long](entId(2, 0))
      )
    ),
    (
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1)
      )),
      entRepo(Iterable(
        ent(1, 0),
        ent(1, 1)
      )),
      (
        Set[Long]() -> Set[Long]()
        )
    )
  )

  "Entity repo diff tests" should "pass" in {
    forAll(diffTestData) { (repo1, repo2, expDiff) =>
      val diff = NaEnRepo.diff(repo1, repo2)

      diff shouldBe expDiff
    }
  }
}
