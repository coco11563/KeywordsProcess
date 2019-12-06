package pub.sha0w

import java.util.Properties

import com.mysql.cj.jdbc.Driver
import org.apache.commons.dbcp.DriverConnectionFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import pub.sha0w.ETL.KeywordsProcess.fieldIndex
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}
import pub.sha0w.ETL.Utils.StringUtils

import scala.collection.mutable

object wordCount {
  val mysqladd = "jdbc:mysql://192.168.3.131:3306/NSFC_KEYWOR_DB"
  Class.forName("com.mysql.cj.jdbc.Driver")
  val property = new Properties
  property.put("user","root")
  property.put("password", "Bigdata,1234")
  property.put("driver","com.mysql.cj.jdbc.Driver")

  val conn = new DriverConnectionFactory(new Driver(), mysqladd, property).createConnection()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .getOrCreate()
    val mysqladd = "jdbc:mysql://192.168.3.131:3306/NSFC_KEYWOR_DB"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "Bigdata,1234")
    property.put("driver","com.mysql.cj.jdbc.Driver")
    val newApplication = spark.
      read.
      format("jdbc").
      jdbc(mysqladd,args(0), property)
    //origin.2019_application

    val oldKeyword = spark.
      read.
      format("jdbc").
      jdbc(mysqladd,args(1), property) //origin.2019_provided_keyword
    val recently_schema = oldKeyword.schema
    //得到去年使用的关键字数据
    val oldKeywordSet = oldKeyword.rdd.map(R => (R.getAs[String](fieldIndex(recently_schema,"APPLYID")),
      R.getAs[String](fieldIndex(recently_schema,"research_field".toUpperCase())),
      R.getAs[String](fieldIndex(recently_schema,"keyword".toUpperCase))))
      .filter(str => str._3 != null)
      .map(f => {(f._1, f._2, StringUtils.totalSplit(f._3))}) //args(2) : ","
      .map(f => {f._3.map(a => (f._1, f._2, a))}).flatMap(a => a)
      .map(r => {
        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
      }).collect().toSet
    val oldKeywordBroadcast = spark.sparkContext.broadcast[Set[HierarchyKeyword]](oldKeywordSet)
    val isOldKeyFilter : HierarchyKeyword => Boolean = (a : HierarchyKeyword) => {
      oldKeywordBroadcast.value.contains(a) // true for this keyword is old contained
    }
    val oldKeywordMap = oldKeywordSet.groupBy(hk => hk.hierarchy).map(f => (f._1, f._2.toList)).toMap

    val newAppschema = newApplication.schema
    //得到并解析今年的关键字
    val newAppRdd = newApplication.rdd.map(r => {
      (r.getAs[String](fieldIndex(newAppschema,"keyword_zh".toUpperCase)),
        r.getAs[String](fieldIndex(newAppschema,"title_zh".toUpperCase)),
        r.getAs[String](fieldIndex(newAppschema,"applyid".toUpperCase)),
        r.getAs[String](fieldIndex(newAppschema,"research_field".toUpperCase)),
        r.getAs[String](fieldIndex(newAppschema,"abstract_zh".toUpperCase)))
    }).filter(tu => {
      tu._1 != null  && tu._3 != null //存在中文关键字
    })
    val hierarchyKeywordMap: Map[Hierarchy, List[HierarchyKeyword]] = newAppRdd.map(line => {
      //new hierarchy(ros, applyid) : tuple(abs, title, keyword)
      line._1.split("；").map(str =>  (new Hierarchy(line._4, line._3), str ))
    }).flatMap(f => f).map(pair => {(new HierarchyKeyword(pair._2, pair._1), pair)}).reduceByKey((a, _) => a)
      .values.groupByKey().map(f => {
      (f._1, f._2.toList.map(str => new HierarchyKeyword(str, f._1) ))
    }).collect().toMap

    val combinationHierarchyKeyworMap : mutable.HashMap[Hierarchy, List[HierarchyKeyword]] = new mutable.HashMap[Hierarchy, List[HierarchyKeyword]]()
    for (p <- hierarchyKeywordMap) {
      if (combinationHierarchyKeyworMap.contains(p._1)) {
        val tmp = combinationHierarchyKeyworMap(p._1)
        combinationHierarchyKeyworMap.update(p._1, tmp union p._2)
      } else {
        combinationHierarchyKeyworMap.put(p._1, p._2)
      }
    }
    for (p <- oldKeywordMap) {
      if (combinationHierarchyKeyworMap.contains(p._1)) {
        val tmp = combinationHierarchyKeyworMap(p._1)
        combinationHierarchyKeyworMap.update(p._1, tmp union p._2)
      } else {
        combinationHierarchyKeyworMap.put(p._1, p._2)
      }
    }
    val hierarchyKeywordMapBroadcast = spark.sparkContext.broadcast[Map[Hierarchy, List[HierarchyKeyword]]](
      combinationHierarchyKeyworMap.toMap
    )

    val newAppCorpusMap: Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])] =
      newAppRdd.map(line => {
        //new hierarchy(ros, applyid) : tuple(abs, title, keyword)
        (new Hierarchy(line._4, line._3), (line._5, line._2, line._1))
      }).groupByKey.mapValues(f => {
        f.reduce((pair_a, pair_b) => {
          //abs 1 title 2 keyword 3
          (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2, pair_a._3 + " " + pair_b._3 )
        })
      }).map(f => {
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
    val oldHierarchy: RDD[HierarchyKeyword] = oldKeyword.rdd.map(r => (r.getAs[String]("applyid".toUpperCase), r.getAs[String]("research_field".toUpperCase)
      ,r.getAs[String]("keyword".toUpperCase)))
      .map(p => new HierarchyKeyword(p._3, new Hierarchy(p._2, p._1)))


    // for all count
    val newAppHK = newAppRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => f._2.split("；").filter(str => str.trim != "") // split the keyword and remove blank keyword
        .map(str => (str, f._1))) // keyword - hierarchy
      .flatMap(f => f)
      // f => hK(k,h)
      .map(f => new HierarchyKeyword(f._1, f._2))
      .groupBy(f => f).map(f => f._1)
      .filter(f => {
        //这个filter让不出现在去年的关键词通过
        !isOldKeyFilter(f)
      }).map(f => (f, true))

    val oldAppHK = oldHierarchy.map(f => (f, false))
    val combinationHK: RDD[(HierarchyKeyword, Boolean)] = newAppHK.union(oldAppHK)
    //1 for "是" 2 for 否 -1 for 去年未录用
    val allcountCombinationHK = getAllCount(combinationHK, newAppCorpusMapBroadcast).map(f=>
    {
      val hk = f._1._1
      //abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
      Row.fromTuple(hk.hierarchy.ApplyID, hk.hierarchy.FOS, hk.keyword, f._2, f._3, f._4, f._5, f._6, f._7)
    })
    spark.createDataFrame(allcountCombinationHK,result_schema)
      .write.
      mode(SaveMode.Overwrite)
      .jdbc(mysqladd, s"${args(2)}_keyword_count", property)
  }

  val result_schema = StructType(Array(
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



  def getAllCount(rdd : RDD[(HierarchyKeyword, Boolean)], cacuMap : Broadcast[Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]])
  : RDD[((HierarchyKeyword, Boolean), Int, Int, Int,Int, Int, Int)] = {
    rdd.map(hk => {
      (hk._1.hierarchy.ApplyID.charAt(0), hk)
    }).groupByKey()
      .map(pair => {
        pair._2.toSeq.map(hk => {
          //(abs : title)
          val text = cacuMap.value.getOrElse(hk._1.hierarchy, (Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int]())) // 如果因为applyid 修改导致这里取不到 则___

          //          val text = cacuMap.value.getOrElse(hk._1.hierarchy, Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]())
          val abs = text._1
          val title: Map[HierarchyKeyword, Int] = text._2
          val keyword = text._3
          //
          val abs_f = abs.getOrElse(hk._1, 0)
          val title_f = title.getOrElse(hk._1, 0)
          val keyword_f = keyword.getOrElse(hk._1, 0)
          // keyword, hierarchyKeyword, abstract_f, title_f, keyword_f, isnew
          (hk._1.keyword, hk, abs_f, title_f, keyword_f)
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

  def process(year : String) : Unit = {
    val ps = conn.prepareStatement(s"select KEYWORD_ZH,RESEARCH_FIELD,APPLYID,TITLE_ZH from ${year}_APPLICATION_NEW;")
    val rs = ps.executeQuery()
    val map = new mutable.HashMap[Hierarchy, String]
    while(rs.next()) {

    }
  }
}
