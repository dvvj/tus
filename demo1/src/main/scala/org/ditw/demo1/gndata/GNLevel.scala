package org.ditw.demo1.gndata

object GNLevel extends Enumeration {
  type GNLevel = Value
  val ADM0, ADM1, ADM2, ADM3, ADM4, ADM5, LEAF = Value

  val intMap:Map[GNLevel, Int] = Map(
    ADM0 -> 2,
    ADM1 -> 8,
    ADM2 -> 16,
    ADM3 -> 32,
    ADM4 -> 64,
    ADM5 -> 128,
    LEAF -> 256
  )
}

