package org.ditw.ent0
import org.ditw.exutil1.naenRepo.NaEnRepo

object RepoDiff {

  def main(args:Array[String]):Unit = {
    val repo1 = UfdEnt.ufdRepoFrom("/media/sf_vmshare/ufd-isni.json")
    val repo2 = UfdEnt.ufdRepoFrom("/media/sf_vmshare/tmpadd-ufd-isni.json")

    val diff = NaEnRepo.diff(repo1, repo2)
    println(
      s"Repo1 Only #: ${diff._1.size}; Repo2 Only #: ${diff._2.size}"
    )
  }

}
