package pub.sha0w.xls.Object

import pub.sha0w.ETL.Utils.StringUtils

class Keyword (val applyid : String, val researchField : String, val name : String) extends Serializable {

}
class KeywordSheet (val applyid : String, val researchField : String, val names  : String) extends Serializable  {
  var li : List[Keyword] = StringUtils.totalSplit(names).map(new Keyword(applyid, researchField, _)).toList
}