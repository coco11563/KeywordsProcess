package pub.sha0w.testWordSeg

import org.apdplat.word.{WordFrequencyStatistics, WordSegmenter}
import org.apdplat.word.dictionary.DictionaryFactory
import org.apdplat.word.segmentation.SegmentationAlgorithm
import org.apdplat.word.util.WordConfTools

object test01 {
  WordConfTools.set("dic.path", "C:\\Users\\coco1\\IdeaProjects\\KeywordsProcess\\src\\main\\res\\test_dict.txt");
  DictionaryFactory.reload()

  def main(args: Array[String]): Unit = {
    val wordFrequencyStatistics = new WordFrequencyStatistics();
    wordFrequencyStatistics.setRemoveStopWord(false);
//    wordFrequencyStatistics.setResultPath("word-frequency-statistics.txt");
    wordFrequencyStatistics.setSegmentationAlgorithm(SegmentationAlgorithm.MaximumMatching);
    wordFrequencyStatistics.seg("ABA,CCAAB,D,CM,D,GHHHHHHH")
//    val index = 0
    wordFrequencyStatistics.dump()
    wordFrequencyStatistics.reset()
  }
}
