package org.ditw.demo1.matchers
import org.ditw.demo1.gndata.GNCntry.GNCntry
import org.ditw.demo1.gndata.TGNMap
import org.ditw.matcher.TokenMatchers.TmMatchPProc
import org.ditw.matcher._
import org.ditw.tknr.{TknrHelpers, Tokenizers, Trimmers}

import scala.collection.mutable.ListBuffer

object TmTest1 extends App {
  import org.ditw.demo1.TestData._
  import MatcherHelper._
  def testTm(
    testStrs:Iterable[String]
  ):Unit = {

    testStrs.foreach { str =>
      val mp = MatchPool.fromStr(str, MatcherHelper.testTokenizer, testDict)
      mmgr.run(mp)

      val rng2Ents = testGNSvc.extrEnts(xtrMgr, mp)
      println(rng2Ents)
    }

  }

//  private val addExtraAdm0Tag:TmMatchPProc[String] = (m, tag) => {
//    m.addTag(TagHelper.adm0DynTag(tag))
//    m
//  }

  import org.ditw.demo1.TestData._
  import org.ditw.demo1.gndata.GNCntry._

  testTm(List(
    "Department of Biostatistics, The University of Texas MD Anderson Cancer Center, Houston.",
    "PR China",
    "SAN JUAN PR",
    "CF10 3NB, UK",
    "Cardiff University, Cardiff, CF10 3NB, UK. Email: prokopovichp@cardiff.ac.uk; Center for Biomedical Engineering, Massachusetts Institute of Technology, Cambridge, USA.",
    "Cardinal Stefan Wyszynski University in Warsaw",
    "Boston University, Boston, USA.",
    "University of Nebraska, Lincoln, Center for Plant Science Innovation, United States.",
    "University of Nebraska Medical Center , Omaha, Nebraska.",
    "3300 Whitehaven St., NW, Suite 4100, Washington, DC, 20007, USA.",
    "Department of Psychiatry, Washington University School of Medicine, St Louis, MO, USA.",
    "Department of Psychiatry, Washington University School of Medicine, St. Louis, MO, USA.",
    "Wurzweiler School of Social Work, Yeshiva University, New York, USA.",
    "Department of Public Health Sciences, University of Rochester Medical Center, Rochester, NY, USA.",
    "Department of Obstetrics and Gynaecology, Hôpital Sainte-Justine and Université de Montréal, Montréal (Québec), Canada.less ",
    "Kyonancho, Musashino, Tokyo 180-8602, Japan.",
    "Tama-shi Tokyo-to 206-8512 Japan",
    "Chuo-ku, Fukuoka 810-0051, Japan.",
    "Onga, Fukuoka Japan.",
    "Higashi-ku, Fukuoka 813-0017, Japan.",
    "Miyata Eye Hospital Miyakonojo-shi Miyazaki-ken Japan",
    "Miyata Eye Hospital Miyasaki Japan",
    "Kurume University Japan",
    "Beppu, Oita, 874-0838, Japan",
    "Imizu City, Toyama-ken 937-8585, Japan",
    "Uozu City, Toyama-ken 937-8585, Japan",
    "Uozu City, Toyama 937-8585, Japan",
    "Sanno Hospital (Komatsu), Tokyo, Japan.",
    "Komatsu Japan",
    "Kanazawa Japan",
    "University of Southern California, Los Angeles, California, USA",
    "Chongqing, 400044, China.",
    "Beijing, The People's Republic of China.",
    "Los Angeles, California, USA",
    "Université de Montréal, Montréal (Québec), Canada.",
    "George Washington University, Washington, D.C.",
    "Washington, USA.",
    "Worcester County, Massachusetts, USA.",
    "City of Boston, Massachusetts, USA.",
    "Boston, Massachusetts, USA.",
    "U.S. Secret Service, Forensic Services Division, Washington, DC, USA.",
    "massachusetts",
    "Worcester County",
    "City of Boston"
  ))
}
