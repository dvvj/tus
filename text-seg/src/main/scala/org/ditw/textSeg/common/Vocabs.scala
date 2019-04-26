package org.ditw.textSeg.common
import org.ditw.common.InputHelpers.splitVocabToSet
import org.ditw.common.{Dict, InputHelpers, ResourceHelpers}
import org.ditw.textSeg.catSegMatchers.Cat2SegMatchers
import org.ditw.textSeg.common.CatSegMatchers.Category
import org.ditw.tknr.TknrHelpers

object Vocabs extends Serializable {

  private[textSeg] val _SegSfxs = Set(
    ",", ";", ")"
  )

  private[textSeg] val _SfxsCommas = Set(
    ","
  )
  private[textSeg] val _SegPfxs =
    TknrHelpers._AffIndexChars.map(_.toString).toSet + "("
//  private[textSeg] val _SegPfxs_Bracket = Set(
//    "("
//  )
//  private[textSeg] val _SegSfxs_Bracket = Set(
//    ")"
//  )

  private[textSeg] val _CorpWords = Set(
    "llc", "inc", "ltd",
    "pte ltd", "pty ltd", "pvt ltd", "co ltd",
    "ggmbh", "gmbh", "kgaa",
    "pharmaceuticals", "plc", "corp",
    "srl",
    "s/a"
  )

  private[textSeg] val _UnivWords = Set(
    "university", "univ", "hochschule", "università",
    "universidade",
    "universitário",
    "universitario",
    "université",
    "universitá",
    "universitaria"
  )

  import Category._

  private val _catToResPath = Map(
    Category.Corp -> "cat1",
    Category.Univ -> "cat2",
    Category.Hosp -> "cat3",
    Category.ResInst -> "cat4"
  )

  private[textSeg] def loadStopWords(cat: Category):Set[String] = {
    val resPath = _catToResPath(cat)
    val s1 = ResourceHelpers.loadStrs(s"/$resPath/stopwords.txt").toSet
    val s2 = otherGazWordsAsStopWords(cat)
    s1 ++ s2
  }

  private[textSeg] def loadGazWords(cat: Category, throwIfNotFound:Boolean = true):Set[String] = {
    val resPath = _catToResPath(cat)
    ResourceHelpers.loadStrs(s"/$resPath/gaz.txt", throwIfNotFound).toSet
  }

  private[textSeg] val _catToGazSet:Map[Category, Set[String]] = Category.values.toList.map { cat =>
    cat -> loadGazWords(cat, false)
  }.toMap

  private[textSeg] def otherGazWordsAsStopWords(cat: Category):Set[String] = {
    Category.values.filter(_ != cat)
      .flatMap(_catToGazSet)
  }

  private[textSeg] val _UnivStopWords = loadStopWords(Univ)
  private[textSeg] val _UnivGazWords = _catToGazSet(Univ)
  private val namedSchoolColleges =
    ResourceHelpers.load("/shared/named_school_colleges.json", NamedSchoolCollege.fromJson)
  private val namedSchoolCollegeNames = namedSchoolColleges.flatMap { n =>
    n.aliases :+ n.name
  }.map(_.toLowerCase()).toSet

  private[textSeg] val __UnivSegStopWordsCommon = Set(
    "school",
    "Schools",
    "college",
    "Hospital",
    "Hospitals",
    "Chikushi Hospital",
    "Laboratory",
    "Marine Laboratory",
    "Program",
    "Marine Program",
    "now with",
    "All authors",
    "Center",
    "sw medical center",
    "southwestern medical center",
    "medical center",
    "Cancer Center",
    "Health Center",
    "medical Centre",
    "downstate medical center",
    "medical campus",
    "biomedical campus",
    "medical school",
    "medical college",
    "graduate school",
    "superior school",
    "law school",
    "department",
    "clinical centre",
    "and",
    "&",
    "/",
    "affiliated to",
    "faculty",
    "-",
    "Institute",
    "from"
  ).map(_.toLowerCase()) ++ namedSchoolCollegeNames ++ otherGazWordsAsStopWords(Category.Univ)

  private[textSeg] val _UnivSegStopWordsLeftExtra = Set(
    "of", "at"
  )
//  private[textSeg] val _UnivSegStopWordsRightExtra = Set(
//    "/"
//  )
  private[textSeg] val _UnivSegStopWordsLeft = __UnivSegStopWordsCommon ++ _UnivSegStopWordsLeftExtra
  private[textSeg] val _UnivSegStopWordsRight = __UnivSegStopWordsCommon // ++ _UnivSegStopWordsRightExtra
  private[textSeg] val __univOfVocab =
    ResourceHelpers.loadStrs("/cat2/univ_of_vocab.txt").toSet

  private[textSeg] val _DeptTypes = ResourceHelpers.loadStrs("/shared/depts.txt").toSet
  private[textSeg] val _DeptWords = Set(
    "dept", "department", "departments", "deptartment", "departamento"
  )

  private[textSeg] val _And = Set("and", "&", "und")

  private val allVocabs = Seq(
    _CorpWords,
    _UnivWords,
    _UnivGazWords,
    _UnivStopWords,
    __UnivSegStopWordsCommon,
    _UnivSegStopWordsLeftExtra,
    //_UnivSegStopWordsRightExtra,
    __univOfVocab,
    namedSchoolCollegeNames,
    _And,
    _DeptTypes,
    _DeptWords
  )

  def allWords:Iterable[Iterable[String]] = allVocabs.map(splitVocabToSet)

}
