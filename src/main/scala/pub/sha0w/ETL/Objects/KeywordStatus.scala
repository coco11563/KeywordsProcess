package pub.sha0w.ETL.Objects

class KeywordStatus(val keywordFrequency : Int,
                    val absTextFrequency : Int,
                    val titleTextFrequency : Int,
                    keywordAmount : Int,
                    isNewWord : Boolean) extends Serializable {
  val percentage : Double = keywordFrequency / keywordAmount
  val weight : Double = KeywordStatus.weight(keywordFrequency, absTextFrequency, titleTextFrequency)
  val count : Int = keywordFrequency + absTextFrequency + titleTextFrequency

}
object KeywordStatus {
  def weight (keywordCount : Int, abstractCount : Int, titleCount : Int) : Double = {
    1.0 * keywordCount + 0.2 * abstractCount + 0.4 * titleCount
  }
}
