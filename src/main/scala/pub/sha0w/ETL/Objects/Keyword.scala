package pub.sha0w.ETL.Objects

import pub.sha0w.ETL.Utils.StringUtils

class Keyword(val name : String, iterable : Iterable[(Hierarchy, Int)]) {
  val sq: Seq[(Hierarchy, Int)] = iterable.toSeq
  //1. count 2. abstext 3. titletext
  var hiMap : Map[Hierarchy, (Int, Int, Int)] = sq.map(f => {
    (f._1, (f._2, 0, 0))
  }).toMap
  val amount : Int = hiMap.values.map(f => f._1).sum
  // TODO 如果你想训练数据呢？
  def applyText (set: Map[Hierarchy, (String, String)]) : Unit = {
    for (pair <- hiMap) {
      hiMap = hiMap.updated(pair._1, {
        val absTitle = set(pair._1)
        (pair._2._1, StringUtils.countString(absTitle._1, name),
          StringUtils.countString(absTitle._2, name))
      })
    }
  }
}

object Keyword {
  val amountThreshold : Double = 1.0

  val abstractThreshold : Double = 0.4

  val titleThreshold : Double = 0.2
}
