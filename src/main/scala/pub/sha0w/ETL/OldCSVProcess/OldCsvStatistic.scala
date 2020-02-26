package pub.sha0w.ETL.OldCSVProcess

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import pub.sha0w.ETL.KeywordsProcess.fieldIndex
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}
import pub.sha0w.ETL.Utils.StringUtils

object OldCsvStatistic {
  def main(args: Array[String]): Unit = {
//    for (i <- split_keywords("经穴特异性.C-fos基因表达.脑机制                       ")){
    ////      println(i)
    ////    }
    ////
    val mysqladd = "jdbc:mysql://10.0.202.18:3306/application_processed"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "")
    property.put("driver","com.mysql.cj.jdbc.Driver")
        for (year <- args(0).toInt to args(1).toInt) {
          process_year(year.toString, mysqladd, property)
        }
  }
  def process_year(year:String, mysqladd: String, property: Properties) : Unit = {
    val spark = SparkSession.builder
      .getOrCreate()

    val newApplication = spark.
      read.
      format("jdbc").
      jdbc(mysqladd,s"${year}_APPLICATION_OLD", property)
    //origin.2019_application
    val newAppschema = newApplication.schema
    val newAppRdd = newApplication.rdd.map(r => {
      (r.getAs[String](fieldIndex(newAppschema,"ckeyword")),
        r.getAs[String](fieldIndex(newAppschema,"zh_title")),
        r.getAs[String](fieldIndex(newAppschema,"applyid")),
        null,
        r.getAs[String](fieldIndex(newAppschema,"abstract")))
    })
    val af_split =  newAppRdd.map(line => {
      split_keywords(line._1).map(str =>  (new Hierarchy(line._4, line._3), str))})
//    hierarchy -> keyword str
    println(s"*****afsplit num is ${af_split.count()}")
    val hierarchyKeywordMap: Map[Hierarchy, List[HierarchyKeyword]] = af_split
      .flatMap(f => f)
      .map(pair => {(new HierarchyKeyword(pair._2, pair._1), pair)}).reduceByKey((a, _) => a)
      .values.groupByKey().map(f => {
      (f._1, f._2.toList.map(str => new HierarchyKeyword(str, f._1) ))
    }).collect().toMap
    val hierarchyKeywordMapBroadcast = spark.sparkContext.broadcast[Map[Hierarchy, List[HierarchyKeyword]]](
      hierarchyKeywordMap
    )
    println(hierarchyKeywordMap.head._1.toString)
//    println(hierarchyKeywordMap(new Hierarchy("", "C170106")))
//    println(hierarchyKeywordMap(new Hierarchy("", "C170106")).length)
    val hierarchyKeywords: RDD[HierarchyKeyword] = af_split
      .flatMap(f => f)
      .map(pair => {
        new HierarchyKeyword(pair._2, pair._1)
      }).distinct()

    val newAppCorpusMap: Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])] =
      newAppRdd.map(line => {
        //new hierarchy(ros, applyid) : tuple(abs, title, keyword)
        (new Hierarchy(line._4, line._3), (line._5, line._2, line._1))
      }).groupByKey.mapValues(f => {
        f.reduce((pair_a, pair_b) => {
          //abs 1 title 2 keyword 3
          (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2, pair_a._3 + " " + pair_b._3 )
        })
      }).filter(f => hierarchyKeywordMapBroadcast.value.contains(f._1)).map(f => { //有一些关键词为“”，故不会载入到keyword map中
        val keys = hierarchyKeywordMapBroadcast.value(f._1)
        (f._1, (
          StringUtils.maximumWordCount(keys, f._2._1),
          StringUtils.maximumWordCount(keys, f._2._2) ,
          StringUtils.maximumWordCount(keys, f._2._3)))
      }).collect().toMap

    val newAppCorpusMapBroadcast =
      spark.sparkContext.
        broadcast[Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]](
          newAppCorpusMap
        )

    //1 for "是" 2 for 否 -1 for 去年未录用
    val allcountCombinationHK = getAllCount(hierarchyKeywords, newAppCorpusMapBroadcast).map(f=>
    {
      val hk = f._1     //abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
      Row.fromTuple(hk.hierarchy.ApplyID, hk.hierarchy.FOS, hk.keyword, f._2, f._3, f._4, f._5, f._6, f._7)
    })
    spark.createDataFrame(allcountCombinationHK,result_schema)
      .write.
      mode(SaveMode.Overwrite)
      .jdbc(mysqladd, s"${year}_keyword_count_old", property)

  }

  def check_most(string: String) : String = {
    var max = Int.MinValue
    var c = ""
    for (i <- string.toCharArray) {
      val str = i.toString
      val num = StringUtils.countString(str, string)
      if (num > max) {
        max = num
        c = str
      }
    }
    c
  }

  def split_keywords(string: String) : Array[String] = {
    val tmp = string.trim
    val dot_count = StringUtils.countString(tmp, ".")
    val comma_count = StringUtils.countString(tmp, ",")
    val ch_comma_count = StringUtils.countString(tmp, "，")
    val ch_stop_count = StringUtils.countString(tmp, "；")
    val stop_count = StringUtils.countString(tmp, ";")
    if (dot_count == ch_comma_count && comma_count == ch_stop_count && dot_count == stop_count && dot_count == comma_count) {
//      print(1)
//      val stop = check_most(tmp)
//      if (StringUtils.countString(tmp, stop) > 1)
//        tmp.split(stop)
//      else Array(tmp)
      Array(tmp)
    } else {
//      println(2)
      val max = Array(dot_count, comma_count, ch_comma_count, ch_stop_count, stop_count).max
//      print(max)
      if (max == dot_count){
        tmp.split("\\.")
      } else if (max == comma_count) {
        tmp.split(",")
      } else if (max == ch_comma_count) {
        tmp.split("，")
      } else if (max == ch_stop_count) {
        tmp.split("；")
      } else {
        tmp.split(";")
      }
    }
  }

  def getAllCount(rdd : RDD[HierarchyKeyword], cacuMap : Broadcast[Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]])
  : RDD[(HierarchyKeyword, Int, Int, Int,Int, Int, Int)] = {
    rdd.map(hk => {
      (hk.hierarchy.ApplyID.charAt(0), hk)
    }).groupByKey()
      .map(pair => {
        pair._2.toSeq.map(hk => {
          //(abs : title)
          val text = cacuMap.value.getOrElse(hk.hierarchy, (Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int]())) // 如果因为applyid 修改导致这里取不到 则___

          //          val text = cacuMap.value.getOrElse(hk._1.hierarchy, Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]())
          val abs = text._1
          val title: Map[HierarchyKeyword, Int] = text._2
          val keyword = text._3
          //
          val abs_f = abs.getOrElse(hk, 0)
          val title_f = title.getOrElse(hk, 0)
          val keyword_f = keyword.getOrElse(hk, 0)
          // keyword, hierarchyKeyword, abstract_f, title_f, keyword_f, isnew
          (hk.keyword, hk, abs_f, title_f, keyword_f)
        })
          .groupBy(f =>  f._1).mapValues(f => {
          val abs_f_all: Int = f.map(_._3).sum
          val title_f_all: Int = f.map(_._4).sum
          val keyword_f_all: Int = f.map(_._5).sum
          f.map(tu => {
            //  1,hierarchyKeyword 2, abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall
            (tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all)
          })
        })
          .flatMap(f => f._2)
          .toSeq //return Seq[(String,HierarchyKeyword,Int, Int, Int, Int,Int, Int,Boolean)]
      }).flatMap(f => f)
  }

  val result_schema: StructType = StructType(Array(
    StructField("applyid",StringType, nullable = true),
    StructField("research_field",StringType, nullable = true),
    StructField("keyword",StringType, nullable = false),
    StructField("abstract_f",IntegerType, nullable = false),
    StructField("title_f",IntegerType, nullable = false),
    StructField("keyword_f",IntegerType, nullable = false),
    StructField("abstract_all",IntegerType, nullable = false),
    StructField("title_all",IntegerType, nullable = false),
    StructField("keyword_all",IntegerType, nullable = false)
  ))
}
