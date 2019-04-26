package org.ditw.pmxml.model

case class AAAuSingle(
  valid:Boolean,
  index:Int,
  lastName:String,
  foreName:String,
  initials:Option[String],
  affLocalIds:Seq[Int],
  idfr:Option[Identifier] = None
  ) {

}
