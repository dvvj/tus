package org.ditw.exutil1.naenRepo
import org.ditw.exutil1.naen.NaEn


import scala.collection.mutable.ListBuffer
import collection.mutable
class NaEnRepo[T](
  ents:Iterable[NaEn],
  val desc:String,
  private val categoryMask: NaEnRepo.IdMask,
  private val newEnt2Cat: NaEnRepo.NewEnt2Category[T],
  private val genNewEnt: NaEnRepo.NewEnt[T],
  //private val newEntId:Long => Long,
  private val newCatBaseId:Long
  ) extends Serializable {

  private[naenRepo] val _entMap:mutable.Map[Long, NaEn] = mutable.Map[Long, NaEn]() ++ ents.map { en =>
    en.neid -> en
  }

  def size:Int = _entMap.size

  private val _entCatMap:mutable.Map[Long, List[NaEn]] =
    mutable.Map[Long, List[NaEn]]() ++ ents.map { en =>
      categoryMask(en.neid) -> en
    }.groupBy(_._1).mapValues { p =>
      val sorted = p.map(_._2).toList.sortBy(_.neid)(Ordering[Long].reverse)
      sorted
    }

  def add(ent:T):Unit = {
    val cat = newEnt2Cat(ent)
    if (!_entCatMap.contains(cat)) {
      _entCatMap += cat -> List()
    }

    val newEntId =
      if (_entCatMap(cat).nonEmpty) _entCatMap(cat).head.neid + 1
      else cat | newCatBaseId

    val newEnt = genNewEnt(newEntId, ent)
    _entCatMap(cat) ::= newEnt
    _entMap += newEntId -> newEnt
  }

  private[naenRepo] def catCount:Map[Long, Int] = _entCatMap.mapValues(_.size).toMap
  private[naenRepo] def catMaxMap:Map[Long, Long] = _entCatMap.mapValues(_.head.neid).toMap

  def allEnts:Iterable[NaEn] = _entMap.values

  def traceCat(id:Long):Unit = {
    val cat = categoryMask(id)
    val sorted = _entMap.keySet.filter(eid => categoryMask(eid) == cat)
      .toArray.sorted
    val hexSorted = sorted.map(eid => f"0x$eid%x")
    println(hexSorted.mkString("\n"))
  }

//  def invalid(neid:Long):Unit = {
//    assert(_entMap.contains(neid), println(s"Neid $neid not Found!"))
//
//    val en = _entMap(neid)
//
//  }


//  private val _categoryMaxs:Map[Long, Long] = {
//
//  }

}

object NaEnRepo extends Serializable {
  type IdMask = Long => Long
  type NewEnt2Category[T] = T => Long
  type NewEnt[T] = (Long, T) => NaEn

  def diff[T](repo1:NaEnRepo[T], repo2:NaEnRepo[T]):(Set[Long], Set[Long]) = {
    val ids1 = repo1._entMap.keySet
    val ids2 = repo2._entMap.keySet
    val ids1Only = (ids1 -- ids2).toSet
    val ids2Only = (ids2 -- ids1).toSet

    ids1Only -> ids2Only
  }
}