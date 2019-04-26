package ie.nod.sparkModels

import org.ditw.exutil1.naen.NaEn
import org.json4s.DefaultFormats

case class SegRes(
                   seg:String,
                   rngStr:String,
                   naen:NaEn
                 )
case class AffRes(
                   pmid:Long,
                   localId:Int,
                   md5:String,
                   segCount:Int,
                   res:List[SegRes],
                   affStr:Option[String]
                 ) {
  override def toString: String = {
    s"$pmid-$localId([$md5]) #: ${res.size} (${res.map(_.naen).mkString(",")}"
  }
}

object AffRes extends Serializable {

  def affRes2Json(ssr:AffRes):String = {
    import org.json4s.jackson.Serialization._
    write(ssr)(DefaultFormats)
  }

}
