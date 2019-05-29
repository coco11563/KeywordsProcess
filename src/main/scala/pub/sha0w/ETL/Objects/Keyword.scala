package pub.sha0w.ETL.Objects

import pub.sha0w.ETL.Utils.StringUtils

/**
  * (1) 如果候选关键词在往年都没有被用户使用过（完全新的关键词），
  * 如果关键词在研究方向下出现了2次以上，加权频次在2.0以上，
  * 在研究方向下出现的比例大于0.7（研究方向下加权频次 ÷ 学部下加权频次），
  * 将关键词补充到研究方向下。
  *
  * (2) 如果候选关键词在往年被填写过（但没有被专家采用），
  * 如果关键词在研究方向下出现了15次以上，55次以下，
  * 加权频次在6.0以上，在研究方向下出现的比例大于0.5，
  * 将关键词补充到研究方向下。(需要历年的推荐数据)
  *
  * @param name
  * @param iterable
  * @param beenSelected 去年是否被选中
  *
  */
class Keyword(val name : String, iterable : Iterable[(Hierarchy, Int)], val beenSelected  : Boolean = false) extends Serializable {
  val sq: Seq[(Hierarchy, Int)] = iterable.toSeq
  val amount : Int = sq.map(f=> f._2).sum
  //1. count 2. abstext 3. titletext
  var hiMap : Map[Hierarchy, KeywordStatus] = sq.map(f => {
    (f._1, new KeywordStatus(f._2, 0, 0, amount, beenSelected))
  }).toMap

  // TODO 如果你想训练数据呢？
  def applyText (set: Map[Hierarchy, (String, String)]) : Unit = {
    for (pair <- hiMap) {
      hiMap = hiMap.updated(pair._1, {
        val absTitle = set(pair._1)
        new KeywordStatus(pair._2.keywordFrequency, StringUtils.countString(absTitle._1, name),
          StringUtils.countString(absTitle._2, name), amount, true)
      })
    }
  }

  def keywordFilter : Result = {
    val seq = hiMap.toSeq
    val resultseq = if (!beenSelected) { // 全新词
      seq.filter(pair => {
        pair._2.weight > 2.0 && pair._2.percentage > 0.7 && pair._2.count > 2
      }).map(p => (p._1, p._2,"newword"))
    }
    else { // 历年未被选择
      seq.filter(pair => {
        pair._2.weight > 6.0 && pair._2.count > 15 && pair._2.count < 55 && pair._2.percentage > 0.5
      }).map(p => (p._1, p._2,"notbeenselectedlastyear"))
    }
    new Result(resultseq, name)
  }
}

object Keyword {
  val amountThreshold : Double = 1.0

  val abstractThreshold : Double = 0.4

  val titleThreshold : Double = 0.2
}
