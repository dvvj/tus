package ie.nod.sparkModels

import org.json4s.DefaultFormats

case class SegResOut(
                      seg:String,
                      eid:Long,
                      name:String,
                      gnid:Long,
                      isni:Option[String]
                    )
case class AffResOut(
                      pmid:Long,
                      localId:Int,
                      md5:String,
                      segCount:Int,
                      res:Array[SegResOut]
                    )

object AffResOut extends Serializable {

  private def segRes2Out(segRes: SegRes):SegResOut = {
    SegResOut(
      segRes.seg, segRes.naen.neid, segRes.naen.name, segRes.naen.gnid,
      segRes.naen.attr("isni")
    )
  }

  def affRes2Out(affRes: AffRes):AffResOut = {
    AffResOut(
      affRes.pmid,
      affRes.localId,
      affRes.md5,
      affRes.segCount,
      affRes.res.map(segRes2Out).toArray
    )
  }

  def affResOut2Json(ssr:AffResOut):String = {
    import org.json4s.jackson.Serialization._
    write(ssr)(DefaultFormats)
  }

  def fromJson(j:String):AffResOut = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    parse(j).extract[AffResOut]
  }

}