package org.ditw.exutil1.naen

object TagHelper extends Serializable {

  val BuiltinTagPfx = "NEBI_"
  def builtinTag(sfx:String):String = BuiltinTagPfx + sfx

  val NaEnId_Pfx = "NEId_"

  def NaEnId(id:Long):String = NaEnId_Pfx+id
}
