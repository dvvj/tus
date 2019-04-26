package org.ditw.pmxml.model

import org.json4s.DefaultFormats

case class AAAuAff(
  singleAuthors:Seq[AAAuSingle],
  collAuthors:Seq[AAAuColl],
  affs:Seq[AAAff]
  ) {

}

object AAAuAff extends Serializable {
  val EmptyAuAff = AAAuAff(Seq(), Seq(), Seq())

  private val _fmts = DefaultFormats
  def toJson(auAff:AAAuAff):String = {
    implicit val fmts = _fmts
    import org.json4s.jackson.Serialization.write
    write(auAff)
  }

  def fromJson(json:String):AAAuAff = {
    implicit val fmts = _fmts
    import org.json4s.jackson.JsonMethods._
    parse(json).extract[AAAuAff]
  }

  def createFrom(auList:AuthorList):AAAuAff = {
    val allAffs:IndexedSeq[(String, AffInfo)] = auList.authors.flatMap(_.affInfo)
      .map { aff =>
        aff.aff -> aff
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2).head) // todo: merge affs in case they contain diff info
      .toIndexedSeq.sortBy(_._1)
    val name2Aff = allAffs.indices.map { idx =>
      val name = allAffs(idx)._1
      name -> AAAff(
        idx, allAffs(idx)._2
      )
    }

    val affs = name2Aff.map(_._2)

    val name2AffMap = name2Aff.toMap

    val singleAus = auList.authors.indices.filter(
        idx => auList.authors(idx).isSingleAuthor
      )
      .map { idx =>
        val au = auList.authors(idx)
        val lastName = au.lastName.get
//        if (au.foreName.isEmpty)
//          println("ok")
        val foreName = au.foreName.getOrElse("")
        val affLocalIds = au.affInfo.map(aff => name2AffMap(aff.aff).localId)
        AAAuSingle(
          au.isValid,
          idx,
          lastName, foreName, au.initials,
          affLocalIds,
          au.idfr
        )
      }

    val collAus = auList.authors.indices.filter(
        idx => auList.authors(idx).isCollective
      )
      .map { idx =>
        val collAu = auList.authors(idx)
        AAAuColl(collAu.collectiveName.get, idx)
      }

    AAAuAff(
      singleAus,
      collAus,
      affs
    )
  }

}