package org.ditw.textSeg.catSegMatchers
import org.ditw.matcher.MatchPool
import org.ditw.textSeg.TestHelpers
import org.ditw.textSeg.common.AllCatMatchers.mmgrFrom
import org.ditw.textSeg.common.Tags._
import org.ditw.textSeg.common.Vocabs
import org.ditw.tknr.TknrHelpers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class Cat2SegMatchersTest extends FlatSpec with Matchers with TableDrivenPropertyChecks {

  private def testDataEntr(
    inStr:String,
    ranges:(Int, Int, Int)*
  ):(String, String, Set[(Int, Int, Int)]) = {
    (
      inStr, TagGroup4Univ.segTag, ranges.toSet
    )
  }

  //  part-00245:University Biomedical Campus, Rome, Italy.
  //  part-00245:University Biomedical Campus, Rome, Italy. massimo.ciccozzi@iss.it.
  //  part-00245:University of Biomedical Campus of Rome, Rome, Italy.
  //  part-00245:University of Biomedical Campus, Rome, Italy.

  //  University of Cambridge Hutchison/MRC Research Centre Cambridge Biomedical Campus

  private val univSegtestData = Table(
    ("inStr", "tag", "expRanges"),
    testDataEntr(
      "Department of Epidemiology and Medicine Kyoto Prefectural University of Medicine",
      (0, 5, 10)
    ),
    testDataEntr(
      "565-0871 Osaka University",
      (0, 1, 3)
    ),
    testDataEntr(
      "Gifu Pharmaceutical University 1-25-4",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of California-San Diego",
      (0, 0, 3)
    ),
    testDataEntr(
      "Department of Epidemiology and Environmental Health Juntendo University Graduate School of Medicine Tokyo Japan.",
      (0, 6, 8)
    ),
    testDataEntr(
      "Wayne State University/Detroit Medical Center, Detroit, MI, USA.",
      (0, 0, 3)
    ),
//    testDataEntr(
//      "†Department of Biology/Chemistry University of Osnabrück",
//      (0, 0, 2)
//    ),
//    testDataEntr(
//      "Departments of Molecular, Cellular, and Developmental Biology, Chemistry, and Pharmacology Yale University",
//      (0, 10, 12)
//    ),
    testDataEntr(
      "Department of Cellular Biology and Anatomy Georgia Regents University",
      (0, 6, 9)
    ),
    testDataEntr(
      "Department of Anatomy First Military Medical University, Guangzhou 510515, China.",
      (0, 3, 7)
    ),
    testDataEntr(
      "Department of Anatomy Erasmus University Rotterdam, The Netherlands. DeZeeuw@Anat.Fgg.Eur.Nl",
      (0, 3, 6)
    ),
    testDataEntr(
      "Bradford University. jateare@bradford.ac.uk",
      (0, 0, 2)
    ),
    testDataEntr(
      "Bradford University Law School, Bradford, West Yorkshire, UK.",
      (0, 0, 2)
    ),
    testDataEntr(
      "Departments of Developmental Biology, Chemistry, and Pharmacology Yale University",
      (0, 7, 9)
    ),
    testDataEntr(
      "‡Department of Biology and Chemistry Yamaguchi University",
      (0, 5, 7)
    ),
    testDataEntr(
      "‡Department of Biology & Chemistry Yamaguchi University",
      (0, 5, 7)
    ),
    testDataEntr(
      "Boğazici University Biology Department",
      (0, 0, 2)
    ),
    testDataEntr(
      "Department of Biology Boğazici University",
      (0, 3, 5)
    ),
    testDataEntr(
      "Department of Chemical and Biomolecular Engineering North Carolina State University,",
      (0, 6, 10)
    ),
    testDataEntr(
      "Biomedicum Helsinki University of Helsinki",
      (0, 2, 5)
    ),
    testDataEntr(
      "\"C.I. Parhon\" National Institute of Endocrinology,\"Carol Davila\" University of Medicine and Pharmacy",
      (0, 6, 13)
    ),
    testDataEntr(
      "3Department of Biomedicine,Federal University of Uberlândia,Uberlândia,Brazil.",
      (0, 3, 7)
    ),
    testDataEntr(
      "Department of Biochemistry and Molecular Biology and Institute of Biomedicine from the University of Barcelona",
      (0, 11, 15)
    ),
    testDataEntr(
      "*Midwifery Department,Shahid Sadoughi University of Medical Sciences,Yazd,Iran.",
      (0, 2, 8)
    ),
    testDataEntr(
      "#Department of Internal Medicine, Medical College of Georgia, Georgia Regents University, Augusta, Georgia 30912, United States.",
      (0, 8, 11)
    ),
    testDataEntr(
      "A. Gemelli Hospital-Catholic University of Rome",
      (0, 4, 8)
    ),
    testDataEntr(
      "University of Pittsburgh Medical Center in Italy" // it's considered cat3
    ),
    testDataEntr(
      "Key Laboratory of Biorheological Science and Technology (Chongqing University)",
      (0, 7, 9)
    ),
    testDataEntr(
      "\"Iuliu Hatieganu\" University of Medicine and Pharmacy Department of Medical Sciences Cluj-Napoca Romania \"Iuliu Hatieganu\" University of Medicine and Pharmacy",
      (0, 0, 7),
      (0, 13, 20)
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine and Farmacy, Obstetrics-Gynaecology Clinic",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine Bucharest, Department II",
      (0, 0, 5)
    ),
    testDataEntr(
      "\"Carol Davila\" University of medicine and Pharmacy bucharest",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Gr.T. Popa\" University of Medicine and Pharmacy of Iasi, 700503 Iasi",
      (0, 0, 9)
    ),
    testDataEntr(
      "University of Medicine and Dentistry of New Jersey",
      (0, 0, 8)
    ),
    testDataEntr(
      "\"Carol Davila'' University of Medicine and Pharmacy, Bucharest, Romania.",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Iuliu Hațieganu\" University of Medicine and Pharmacy Cluj-Napoca, Romania",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Hippocration\" General Hospital †2nd University",
      (0, 3, 5)
    ),
    testDataEntr(
      "\"Biomolécules et Biotechnologies Végétales,\" Université François-Rabelais de Tours",
      (0, 4, 8)
    ),
    testDataEntr(
      "University of Texas Medical Branch",
      (0, 0, 5)
    ),
    testDataEntr(
      "University of Colorado Denver and Health Science Center",
      (0, 0, 3)
    ),
    testDataEntr(
      "Social and Administrative Pharmacy Touro University California College of Pharmacy",
      (0, 4, 7)
    ),
    testDataEntr(
      "College of Pharmacy Touro University California",
      (0, 3, 6)
    ),
    testDataEntr(
      "University of Cambridge Biomedical Campus",
      (0, 0, 3)
    ),
    testDataEntr(
      "Boston University Medical Campus",
      (0, 0, 2)
    ),
    testDataEntr(
      "University of Oldenburg Medical Campus, School of Medicine",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Texas Southwestern Medical Center at Dallas",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Texas SW Medical Center at Dallas",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Colorado Anschutz Medical Campus.",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Colorado-Anschutz Medical Campus, Aurora, CO, USA.",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Texas - Southwestern Medical Center, Dallas, TX, USA.",
      (0, 0, 3)
    ),
    testDataEntr(
      "Northern Arizona University- Phoenix Biomedical Campus",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Pennsylvania Medical Center",
      (0, 0, 3)
    ),
    testDataEntr(
      "The Graduate School and University Center of The City University of New York",
      (0, 7, 13)
    ),
    testDataEntr(
      "1 Department of Urology, Weill Medical College of Cornell University",
      (0, 8, 10)
    ),
    testDataEntr(
      "University of Thessalia, Medical School",
      (0, 0, 3)
    ),
    testDataEntr(
      "Drexel University School of Public Health, Dept. of Community Health & Prevention",
      (0, 0, 2)
    ),
    testDataEntr(
      "The University of Texas Graduate School of Biomedical Sciences at Houston",
      (0, 0, 4)
    ),
    testDataEntr(
      "Matsumoto Dental University Graduate School of Oral Medicine",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Rochester School of Medicine and Dentistry",
      (0, 0, 3)
    ),
    testDataEntr(
      "Yale University School of Medicine",
      (0, 0, 2)
    ),
    testDataEntr(
      "Department of Surgery, University of Vermont Medical College",
      (0, 3, 6)
    ),
    testDataEntr(
      "University of Heidelberg Medical School",
      (0, 0, 3)
    ),
    testDataEntr(
      "Robert Gordon University, School of Nursing and Midwifery",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Gdańsk and Medical University of Gdańsk",
      (0, 0, 3),
      (0, 4, 8)
    ),
    testDataEntr(
      "University of Thessalia, Medical School",
      (0, 0, 3)
    ),
    testDataEntr(
      "University of Arkansas for Medical Sciences",
      (0, 0, 6)
    ),
    testDataEntr(
      "Drexel University School of Public Health, Dept. of Community Health & Prevention",
      (0, 0, 2)
    ),
    testDataEntr(
      "wyss institute for biologically inspired engineering at harvard university",
      (0, 7, 9)
    ),
    testDataEntr(
      "harvard university faculty of arts and sciences center for systems biology",
      (0, 0, 2)
    ),
    testDataEntr(
      "Swiss Federal Institute of Technology and University of Zurich",
      (0, 6, 9)
    ),
    testDataEntr(
      "Georgia Institute of Technology and Emory University School of Medicine Atlanta",
      (0, 5, 7)
    ),
    testDataEntr(
      "Massachusetts Institute of Technology and Harvard University",
      (0, 5, 7)
    ),
    testDataEntr(
      "\"12 de Octubre\" University Hospital"
    ),
    testDataEntr(
      "\"A. Gemelli\" University Hospital Foundation"
    ),
    testDataEntr(
      "\"Dr. Carol Davila\" Central Military University Emergency Hospital"
    ),
    testDataEntr(
      "\"Dr Carol Davila\" Central University Emergency Military Hospital"
    ),
    testDataEntr(
      "University of Pittsburgh Medical Center"
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine and Pharmacy, Obstetrics-Gynaecology Clinic",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine & Pharmacy Bucharest",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine, Obstetrics-Gynaecology Clinic",
      (0, 0, 5)
    ),
    testDataEntr(
      "\"Dipartimento di Medicina Clinica (Clinica Malattie Infettive e Tropicali)\" \"Sapienza\" Università di Roma",
      (0, 9, 13)
    ),
    testDataEntr(
      "\"Carol Davila\" University of Medicine and Pharmacy-Bucharest",
      (0, 0, 7)
    ),
    testDataEntr(
      "\"GR.T. Popa\" University of Medicine and Pharmacy Iasi, Faculty of Medicine",
      (0, 0, 7)
    ),
    testDataEntr(
      "Elias University Emergency Hospital"
    ),
    testDataEntr(
      "\"Elias\" University Emergency Hospital"
    ),
    testDataEntr(
      "Southampton General Hospital, (Southampton University Hospitals NHS Trust)"
    ),
    testDataEntr(
      "A. Meyer University Children's Hospital"
    ),
    testDataEntr(
      "A. Meyer\" University Children's Hospital"
    ),
    testDataEntr(
      "Brackenridge Hall 2.108 301 University Boulevard"
    ),
    testDataEntr(
      "Baylor Heart & Vascular Institute and the Departments of Pathology and Medicine, Baylor University Medical Center",
      (0, 12, 14)
    ),
    testDataEntr(
      "Royal Free and University College Medical School (University College London)",
      (0, 0, 7),
      (0, 7, 10)
    ),
    testDataEntr(
      "Charing Cross and Westminster Medical School (University of London) London UK.",
      (0, 6, 9)
    ),
    testDataEntr(
      "(University of Queensland) Royal Brisbane Hospital",
      (0, 0, 3)
    ),
    testDataEntr(
      "\"F. Magrassi - A. Lanzara\" Second University of Naples",
      (0, 5, 9)
    ),
    testDataEntr(
      "''F.Magrassi, A.Lanzara'' Seconda Università di Napoli",
      (0, 2, 6)
    ),
    testDataEntr(
      "\"F. Magrassi and A. Lanzara\" University of Campania \"Luigi Vanvitelli\" Second University of Naples",
      (0, 5, 10),
      (0, 10, 14)
    ),
    testDataEntr(
      "the A.T. Still Research Institute at A.T. Still University (Drs Snider and Degenhardt and Ms Johnson)",
      (0, 6, 9)
    ),
    testDataEntr(
      "Department of Chemistry and Biochemistry Bradley University",
      (0, 5, 7)
    ),
    testDataEntr(
      "Brain Hospital affiliated to Nanjing Medical University",
      (0, 4, 7)
    ),
    testDataEntr(
      "Department of Biology and Spinal Cord and Brain Injury Research Center University of Kentucky",
      (0, 11, 14)
    ),
    testDataEntr(
      "Brain Research Institute Niigata University",
      (0, 3, 5)
    ),
    testDataEntr(
      "Yonsei University Severance Hospital"
    ),
    testDataEntr(
      "Department of Brain Science University of Ulsan College of Medicine",
      (0, 4, 7)
    ),
    testDataEntr(
      "Temple University School of Medicine",
      (0, 0, 2)
    ),
    testDataEntr(
      "Georgetown University/Providence Hospital Family Medicine Residency Program, Washington, DC, USA.",
      (0, 0, 2)
    ),
    testDataEntr(
      "1 Duke University Medical Center Durham",
      (0, 1, 3)
    ),
    testDataEntr(
      "Department of Psychiatry and Behavioral Sciences Stanford University School of Medicine Stanford CA USA.",
      (0, 6, 8)
    ),
    testDataEntr(
      "Department of Neurology, Hematology, Metabolism, Endocrinology and Diabetology Yamagata University",
      (0, 8, 10)
    ),
    testDataEntr(
      "Department of Anesthesiology Duke University Medical Center Durham, NC.",
      (0, 3, 5)
    ),
    testDataEntr(
      "Marine Biological Laboratory, Boston University Marine Program",
      (0, 3, 5)
    ),
    testDataEntr(
      "Marine Biological Laboratory, Boston University Marine Program",
      (0, 3, 5)
    ),
    testDataEntr(
      "University of Iowa Roy J. and Lucille A. Carver College of Medicine",
      (0, 0, 3)
    ),
    testDataEntr(
      "Columbia University Mailman School of Public Health",
      (0, 0, 2)
    ),
    testDataEntr(
      "Cornell University Weill Medical College",
      (0, 0, 2)
    ),
    testDataEntr(
      "Tokyo Medical and Dental University Graduate School",
      (0, 0, 5)
    ),
    testDataEntr(
      "Karmanos Cancer Institute/Wayne State University",
      (0, 4, 7)
    ),
    testDataEntr(
      "Case Western Reserve University Schools of Medicine",
      (0, 0, 4)
    ),
    testDataEntr(
      "a Baylor University",
      (0, 1, 3)
    )
  )

  private val mmgr = mmgrFrom(
    TestVocabs.AllVocabDict,
    Cat2SegMatchers.segMatchers(TestVocabs.AllVocabDict)
  )
  "Cat2 seg matchers test" should "pass" in {
    forAll(univSegtestData) { (inStr, tag, expRanges) =>
      TestHelpers.runAndVerifyRanges(
        TestVocabs.AllVocabDict,
        mmgr, inStr, tag, expRanges
      )
    }
  }
}
