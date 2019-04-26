package org.ditw.textSeg.catSegMatchers
import org.ditw.common.{Dict, InputHelpers}
import org.ditw.textSeg.common.Vocabs.allWords

object TestVocabs extends Serializable {

  private [textSeg] val AllVocabDict:Dict = InputHelpers.loadDict(allWords)


}
