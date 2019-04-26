package org.ditw.demo1.gndata
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.GNLevel.GNLevel

object GNColls extends Serializable {
  private class GNColl(
    val level:GNLevel,
    val self:Option[GNEnt],
    private[demo1] var subAdms:IndexedSeq[String],
    val gents:Map[Long, GNEnt]
  ) extends TGNColl {
    private[gndata] def updateSubAdms(newVal:IndexedSeq[String]):Unit =
      subAdms = newVal

    def name2Id(admMap:Map[String, TGNColl]):Map[String, IndexedSeq[Long]] = {
      val children:IndexedSeq[(String, IndexedSeq[Long])] = subAdms
        .flatMap(subAdm => admMap(subAdm).name2Id(admMap).toIndexedSeq)

      val curr = (gents.values ++ self).flatMap { gen =>
        gen.queryNames.map(n => n.toLowerCase() -> IndexedSeq(gen.gnid))
      }
      (children ++ curr).groupBy(_._1)
        .toIndexedSeq
        .map(p => p._1 -> p._2.flatMap(_._2).distinct)
        .toMap
    }

    def id2Ent(admMap:Map[String, TGNColl]):Map[Long, GNEnt] = {
//      if (self.nonEmpty && self.get.countryCode == "DE")
//        println("ok")
//      if (self.nonEmpty && self.get.gnid == 6547410L)
//        println("ok")
      val children = subAdms
        .flatMap(subAdm => admMap(subAdm).id2Ent(admMap))
      gents ++ children ++ self.map(e => e.gnid -> e)
    }
  }

  private val EmptyIds = IndexedSeq[Long]()
  private val EmptyEnts = IndexedSeq[GNEnt]()
  private class GNCollMap(
    _level:GNLevel,
    _self:GNEnt,
    _subAdms:IndexedSeq[String],
    _gents:Map[Long, GNEnt],
    aliasMap:Map[Long, Iterable[String]],
    val admMap:Map[String, TGNColl]
  ) extends GNColl(_level, Option(_self), _subAdms, _gents) with TGNMap {
//    private val map = childrenMap
    def byId(gnid:Long):Option[GNEnt] = {
      idMap.get(gnid)
    }
    val countryCode:GNCntry = GNCntry.withName(_self.countryCode)

    private def assignOrphanedAdms:Set[String] = {
      val (orphanedAdms, managedAdms) = {
        val allChildren = childAdms(admMap)
        val diff = admMap.keySet -- allChildren
//        println(diff.size)
        diff -> allChildren
      }
      val updateMap = orphanedAdms.toIndexedSeq.map { oadm =>
        var nadm = SrcData.admMinus1Code(oadm)
        var found = false
        while (!found && nadm.nonEmpty) {
          if (managedAdms.contains(nadm.get))
            found = true
          else
            nadm = SrcData.admMinus1Code(nadm.get)
        }
        if (found) {
          nadm.get -> oadm
        }
        else {
          throw new RuntimeException(s"Cannot found parent adm for $oadm")
        }
      }.groupBy(_._1).mapValues(_.map(_._2))

      updateMap.foreach { p =>
        val (parent, children) = p
        val newChildren = admMap(parent).subAdms ++ children
        admMap(parent).updateSubAdms(newChildren)
      }

      // verify
//      {
//        val allChildren = childAdms(admMap)
//        val diff = admMap.keySet -- allChildren
//        println(diff.size)
//      }
      orphanedAdms
    }

    private val _orphanedAdms = assignOrphanedAdms

    val admIdMap:Map[String, Map[Long, GNEnt]] = {
      if (aliasMap.contains(_self.gnid))
        _self.addAliases(aliasMap(_self.gnid))
      val subAdmMap = _subAdms.map { sadm =>
        val m = admMap(sadm).id2Ent(admMap)
        // update aliases
        aliasMap.foreach { p =>
          val (gnid, aliases) = p
          if (m.contains(gnid)) {
            m(gnid).addAliases(aliases)
          }
        }
        sadm -> m
      } :+ (countryCode.toString -> _gents)
      subAdmMap.toMap
    }

    val admNameMap:Map[String, Map[String, IndexedSeq[Long]]] = {
      val subAdmMap = _subAdms.map { sadm =>
        val m = admMap(sadm).name2Id(admMap)
        sadm -> m
      }

      subAdmMap.toMap
    }

    def directSubAdms:Vector[String] = {
      admNameMap.keySet.toVector.sorted
    }


    def idsByName(name:String, adm:String):IndexedSeq[Long] = {
      if (admNameMap.contains(adm)) {
        val ids = admNameMap(adm).getOrElse(name, EmptyIds)
        ids
      }
      else EmptyIds
    }

    private def _byName(name:String, adm:String):IndexedSeq[GNEnt] = {
      if (admNameMap.contains(adm)) {
        val ids = admNameMap(adm).getOrElse(name, EmptyIds)
        val m = admIdMap(adm)
        ids.map(m)
      }
      else EmptyEnts
    }

    def byName(name:String, adm:String):IndexedSeq[GNEnt] = _byName(name.toLowerCase(), adm)

    def byName(name:String):IndexedSeq[GNEnt] = {
      val lower = name.toLowerCase()
      admNameMap.keySet.flatMap { adm => _byName(lower, adm) }
        .toIndexedSeq
    }

    val idMap:Map[Long, GNEnt] = {
      admIdMap.flatMap(_._2) ++ _gents
    }

  }

  def adm0(
    ent:GNEnt,
    subAdms:IndexedSeq[String],
    gents:Map[Long, GNEnt],
    admMap:Map[String, TGNColl],
    aliasMap:Map[Long, Iterable[String]]
  ):TGNMap = {
    new GNCollMap(GNLevel.ADM0, ent, subAdms, gents, aliasMap, admMap)
  }

  def admx(
    level:GNLevel,
    ent:Option[GNEnt],
    subAdms:IndexedSeq[String],
    gents:Map[Long, GNEnt]
  ):TGNColl = {
    new GNColl(level, ent, subAdms, gents)
  }
}

