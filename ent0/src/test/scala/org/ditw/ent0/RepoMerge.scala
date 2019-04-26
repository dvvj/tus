package org.ditw.ent0
import org.ditw.exutil1.naen.NaEn
import org.ditw.exutil1.naenRepo.NaEnRepo

object RepoMerge {
  def main(args:Array[String]):Unit = {

    val orig = UfdEnt.ufdRepoFrom("/media/sf_vmshare/ufd-isni.json")
    import org.ditw.common.GenUtils._
    val newEntities = readJson("/media/sf_vmshare/new-ufd-isni.json", NaEn.fromJsons)

    println(s"Pre #: ${orig.size}; to add #: ${newEntities.length}")

    newEntities.foreach(orig.add)
    println(s"Post #: ${orig.size}")

    val resEnts = orig.allEnts.toArray

    writeJson("/media/sf_vmshare/merged-ufd-isni.json", resEnts, NaEn.toJsons)
  }
}
