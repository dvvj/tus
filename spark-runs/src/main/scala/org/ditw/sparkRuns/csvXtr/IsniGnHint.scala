package org.ditw.sparkRuns.csvXtr
import org.json4s.DefaultFormats

case class IsniGnHint(strInName:String, gnid:Long) {

}

object IsniGnHint extends Serializable {
  def load(json:String):Array[IsniGnHint] = {
    import org.json4s.jackson.JsonMethods._
    implicit val fmt = DefaultFormats
    val p = parse(json)
    p.extract[Array[IsniGnHint]]
  }
}
