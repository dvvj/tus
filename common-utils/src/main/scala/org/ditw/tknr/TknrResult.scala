package org.ditw.tknr
import org.ditw.common.Dict

/**
  * Created by dev on 2018-10-26.
  */
import org.ditw.common.TypeCommon._
class TknrResult(
                  val orig:String,
                  val dict:Dict,
                  val linesOfTokens: IndexedSeq[SeqOfTokens]
                ) extends Serializable {
  val encoded:IndexedSeq[Array[DictEntryKey]] = {
    linesOfTokens.map(_.tokens.map(t => dict.enc(t.content)).toArray)
  }
}

