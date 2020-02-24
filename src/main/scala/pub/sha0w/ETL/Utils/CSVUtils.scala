package pub.sha0w.ETL.Utils

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}

import scala.collection.mutable
import scala.io.{BufferedSource, Source}
import scala.reflect.io.File

object CSVUtils {

  val src = Source.fromFile("/etc/passwd")


  def process_year(year : String) : Unit = {
    val base_file = "src/main/res/1989-2018"
    val src = Source.fromFile(base_file + "/" + year)
    val hks = process_csv(src)
    val corpora_summary = aggregate_copora(src).map(strs => (strs._1, strs._2._2))
    val corpora_title = aggregate_copora(src).map(strs => (strs._1, strs._2._1))
    val summary_combination = corpora_map_build(corpora_summary)
    val title_combination = corpora_map_build(corpora_title)

  }
  def corpora_map_build(corpora : Iterator[(Hierarchy, String)]) : Map[Hierarchy, String] = {
    val summary_combination = new mutable.HashMap[Hierarchy, String]()
    corpora.foreach(pair => {
      if (summary_combination.contains(pair._1)) {
        val tmp = summary_combination.get(pair._1)
        summary_combination.update(pair._1, tmp + pair._2)
      } else {
        summary_combination.put(pair._1, pair._2)
      }
    })
    summary_combination.toMap
  }
//  def hk_num_build(hks : List[HierarchyKeyword], combination : Map[Hierarchy, String]) : Map[HierarchyKeyword, (Int, Int)] = {
//    val summary_combination = new mutable.HashMap[Hierarchy, (Int, Int)]()
//    hks.foreach(hks => {
//      if (summary_combination.contains(hks.hierarchy)) {
//        val tmp: String = combination(hks.hierarchy)
//        StringUtils.maximumWordCount()
//      } else {
//        summary_combination.put(pair._1, pair._2)
//      }
//    })
//  }


  def process_csv(source : BufferedSource) : Iterator[HierarchyKeyword] = {
    val iter: Iterator[Array[String]] = source.getLines().drop(1).map(_.split("\t"))
    val sample = new Array[Array[String]](10)
    iter.copyToArray(sample, 1, 10)
    val sep = test_sample(sample)
    val set = mutable.HashSet[HierarchyKeyword]()
    iter.flatMap(strs => {
      seperate_keywords(strs(3), sep).map(str => {
        new HierarchyKeyword(str, new Hierarchy(null, strs(0)))
      })
    }).foreach(hk => set.add(hk))
    set.toIterator
  }

  def aggregate_copora(source: BufferedSource) : Iterator[(Hierarchy,(String,String))] = {
    val iter: Iterator[Array[String]] = source.getLines().drop(1).map(_.split("\t"))
    iter.map(f => (new Hierarchy(null, f(0)),(f(1),f(4))))
  }


  def seperate_keywords(strs : String, point : String) : Seq[String] = {
    strs.split(point)
  }

  def test_sample(sample : Array[Array[String]]) : String = {
    var dot_count = 0
    var comma_count = 0
    sample.foreach(strs => {
      val kws = strs(3)
      dot_count += StringUtils.countString(kws, ".")
      comma_count += StringUtils.countString(kws, "；")
    })
    if (dot_count > comma_count) "."
    else ","
  }
}
