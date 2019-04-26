package ie.nod.sparkModels
import org.ditw.demo1.gndata.GNSvc
import org.json4s.DefaultFormats

case class PmAffGeo(pmid:Long, eid:Long, gnid:Long, lat:Double, lon:Double, name:String) {

}

object PmAffGeo extends Serializable {

  def toJson(ssr:PmAffGeo):String = {
    import org.json4s.jackson.Serialization._
    write(ssr)(DefaultFormats)
  }

  def affOut2PmAffGeo(affOut:AffResOut, gnsvc:GNSvc):Array[PmAffGeo] = {
    affOut.res.map(r => r.eid -> r).toMap
      .map { p =>
        val r = p._2
        val gn = gnsvc.entById(r.gnid).get
        PmAffGeo(affOut.pmid, r.eid, r.gnid, gn.latitude, gn.longitude, r.name)
      }.toArray
  }
}