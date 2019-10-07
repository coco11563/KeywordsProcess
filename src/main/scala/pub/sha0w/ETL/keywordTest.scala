package pub.sha0w.ETL

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import pub.sha0w.ETL.KeywordsProcess.fieldIndex
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}
import pub.sha0w.ETL.Utils.StringUtils

object keywordTest {
  private val logger: Logger = LoggerFactory.getLogger(keywordTest.getClass)
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("keywordProcess")
//      .set("spark.driver.maxResultSize","2g")
//      .set("hive.metastore.uris", args(4))
    System.setProperty("hive.metastore.uris", args(4))
    logger.debug(args.toString)
//    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
      .config("spark.sql.warehouse.dir", "./spark-warehouse")
    .enableHiveSupport()
    .getOrCreate()
//    val sql = new HiveContext(sc)
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    Class.forName(driverName)
    val property = new Properties
    property.put("user","hive")
    property.put("password", "")
    property.put("driver",driverName)
    val serverAdd = "jdbc:hive2://192.168.3.122:2181,192.168.3.121:2181,192.168.3.123:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
    val startTime = System.currentTimeMillis()

    val conn: Connection = DriverManager.getConnection(serverAdd, property)



    val newApplication: DataFrame = spark.read.jdbc(serverAdd,args(0),property) //origin.2019_application
    //alter table cannot fix this problem, try to use jdbc to solve this problem!!! //
    val oldKeyword = spark.read.jdbc(serverAdd,args(1),property) //origin.2019_provided_keyword
    val recently_schema = oldKeyword.schema
    val lastYearRecommand = spark.read.jdbc(serverAdd,args(3),property)

    val lastYearRecSchema = lastYearRecommand.schema
    //middle.m_recommand_keyword_2018
    //这部分得到去年推荐的关键字数据
//    val lastYearRecSet: Set[HierarchyKeyword] = lastYearRecommand.rdd.map(r => {(
//      r.getAs[String](lastYearRecSchema.fieldIndex("applyid")),
//      r.getAs[String](lastYearRecSchema.fieldIndex("research_field")),
//      r.getAs[String](lastYearRecSchema.fieldIndex("keyword"))
//    )}).map(tuple => {
//      new KeywordSheet(tuple._1, tuple._2, tuple._3).li
//    }).flatMap(a => a)
//      .map(k => {(k.applyid, k.researchField, k.name)})
//      .map(r => {
//        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
//      }).collect().toSet


//    val lastYearRecSetBroadcast = sc.broadcast[Set[HierarchyKeyword]](lastYearRecSet)
    //得到去年使用的关键字数据
//    val oldKeywordSet = oldKeyword.rdd.map(R => (R.getAs[String](fieldIndex(recently_schema,"APPLYID")),
//      R.getAs[String](fieldIndex(recently_schema,"research_field".toUpperCase())),
//      R.getAs[String](fieldIndex(recently_schema,"keyword".toUpperCase))))
//      .filter(str => str._3 != null)
//      .map(f => {(f._1, f._2, StringUtils.totalSplit(f._3))}) //args(2) : ","
//      .map(f => {f._3.map(a => (f._1, f._2, a))}).flatMap(a => a)
//      .map(r => {
//        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
//      }).collect().toSet
//    val oldKeywordBroadcast = sc.broadcast[Set[HierarchyKeyword]](oldKeywordSet)
//    val isOldKeyFilter : HierarchyKeyword => Boolean = (a : HierarchyKeyword) => {
//      oldKeywordBroadcast.value.contains(a) // true for this keyword is old contained
//    }


    val tmp_count = newApplication.select("keyword_zh".toUpperCase, "applyid".toUpperCase).
      rdd.map(r => (r.getAs[String](0), r.getAs[String](1)))
      .filter(str => str._1 != null )
      .filter(str => str._1.contains("特殊集合"))
    val tmp_arr = tmp_count.collect()
    for (a <- tmp_arr) println(a)
    println("wwwwwwwwwwww")
    assert(false)
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
    }) // 244272 枚关键词
    //构建基于学科代码的层次语料库

    // 归并关键字到最末级代码
    // keywords : applyid
    //    val keyword_apply_arr = newAppRdd.map(f => StringUtils.totalSplit(f._1).map(s => (s, f._3)).toSeq)
    //      .flatMap(s => s)  //keyword : applyid
    //      .collect()
    //    val keywordApplyMap = new mutable.HashMap[String, mutable.HashSet[String]]
    //    for (pair <- keyword_apply_arr) {
    //      val keyword = pair._1
    //      val applyid = pair._2
    //      if (keywordApplyMap.contains(keyword)) {
    //        keywordApplyMap(keyword).add(applyid)
    //      } else {
    //        keywordApplyMap.put(keyword, new mutable.HashSet[String]())
    //        keywordApplyMap(keyword).add(applyid)
    //      }
    //    }

    val newAppCorpusMap: Map[Hierarchy, (String, String, String)] = newAppRdd.map(line => {
      //new hierarchy(ros, applyid) : tuple(abs, title, keyword)
      (new Hierarchy(line._4, line._3), (line._5, line._2, line._1))
    }).groupByKey.mapValues(f => {
      f.reduce((pair_a, pair_b) => {
        (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2, pair_a._3 + " " + pair_b._3 )
      })
    }).collect().toMap
    val newAppCorpusMapBroadcast = spark.sparkContext.broadcast[Map[Hierarchy, (String, String, String)]](newAppCorpusMap)

    //args(3) = ；
    //f._4 = rs , f._3 = applyid
    // order the keywords with hierarchy structure
    val result_mid_rdd = newAppRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => f._2.split("；") // split the keyword
        .map(str => (str, f._1))) // keyword - hierarchy
      .flatMap(f => f)
      // f => hK(k,h)
      .map(f => new HierarchyKeyword(f._1, f._2))
      .groupBy(f => f).map(f => (f._1, f._2.size)).filter(f => {
      //这个filter让不出现在去年的关键词通过
      true
    }).
      // HK, num => {H ,(keyword, num at this research field, isLastYearRecommand)}
      // isLastYearRecommand : false for last year not contain ,
      // isLastYearRecommand : </b>true</b> for last year contain)
      map(
        (f: (HierarchyKeyword, Int)) =>
        {
          // !lastYearRecSetB -> 如果是true 去年未推荐 同时未出现在去年采纳的关键词中 isnewkey
          //                  -> 如果是false 去年推荐 同时未出现在去年采纳的关键词中 isOldKey
          (f._1, f._2, true)
        }
      ).filter(f => f._1.keyword == "特殊集合")

    println("新关键词总数为 : " + result_mid_rdd.count())
    // 这部分modified
    /**
      * 1、先通过applyid 进行group ， 获取相应学部下的关键词情况
      * 2、对于每一个学部group 进行内部计算
      *   - 1 group by 每一个ros
      *   - 2 对于每个ros内的关键词统计（keyword, keywordcount, islastyear）
      *     (1) 统计这个hierarchy（ros,app）下的title count\ abs count
      * 3、对于学部group 计算各关键词出现数量，并broad到每一个ros级别关键词中
      * 4、利用算法规则进行过滤，并将不存在研究方向的过滤到另外一个rdd中
      * 5、组织并输出到Hive中
      * 最终输出形式
      * （末级代码、研究领域、关键词、是否新词[这个必是新词]， 学部下总频次， 研究方向下总频次，
      * 学部下关键词中出现频次， 研究方向下关键词中出现频次，
      * 学部下标题中出现频次 ， 研究方向下标题中出现频次）
      * 这封装成一个</函数>，以方便去年使用的旧词的情况统计
      */
    val result: RDD[(String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Boolean)] = hierarchicalKeyworAnalysis(result_mid_rdd, newAppCorpusMapBroadcast)
    println(result.count())
    println("wwwwwwwwwww")
    result.collect().map(f => {
      println("wwwwwwwwwwwwwwww")
      println(s"applyid is ${f._2.toString}.")
      println(s"abs_f is ${f._3}.")
      println(s"title_f is ${f._4}.")
      println(s"keyword_f is ${f._5}.")
      println(s"absall is ${f._6}")
      println(s"titleall is ${f._7}")
      println(s"keywordall is ${f._8}")
      f
    })
//    val oldHierarchy = oldKeyword.rdd.map(r => (r.getAs[String]("applyid"), r.getAs[String]("research_field")
//      ,r.getAs[String]("keyword")))
//      .map(p => new HierarchyKeyword(p._3, new Hierarchy(p._2, p._1)))
//    val oldResult = oldHierarchicalKeyworAnalysis(oldHierarchy, newAppCorpusMapBroadcast)
//    hiveContext.createDataFrame(result,result_schema).write.mode(SaveMode.Overwrite).saveAsTable(s"middle.m_${args(2)}_keyword_recommend_with_bias")
//
//    hiveContext.createDataFrame(oldResult,result_schema).write.mode(SaveMode.Append).saveAsTable(s"middle.m_${args(2)}_keyword_recommend_with_bias")
  }
  val result_schema = StructType(Array(
    StructField("applyid",StringType, nullable = true),
    StructField("research_field",StringType, nullable = true),
    StructField("status",StringType, nullable = false),
    StructField("keyword",StringType, nullable = false),
    StructField("学部下总频次",IntegerType, nullable = false),
    StructField("研究方向下总频次",IntegerType, nullable = false),
    StructField("学部下关键词中出现频次",IntegerType, nullable = false),
    StructField("研究方向下关键词中出现频次",IntegerType, nullable = false),
    StructField("是否需要补充到上级",BooleanType, nullable = false)
  ))

  def hierarchicalKeyworAnalysis (sourceData : RDD[(HierarchyKeyword, Int, Boolean)],
                                  corpusMap : Broadcast[Map[Hierarchy, (String, String, String)]]): RDD[(String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Boolean)] = {
    sourceData.map((pair: (HierarchyKeyword, Int, Boolean)) => {
      (pair._1.hierarchy.ApplyID.charAt(0), pair)
    }).groupByKey() // 依照学科代码进行group
      .map(pair => {
        pair._2.toSeq.map(hk => {
          //(abs : title)
          val text = corpusMap.value(hk._1.hierarchy)
          val abs = text._1
          val title = text._2
          val keyword = hk._1.keyword
          val abs_f = StringUtils.countString(abs, keyword)
          val title_f = StringUtils.countString(title, keyword)
          // keyword, hierarchy, abstract_f, title_f, keyword_f, isnew
          (keyword, hk._1, abs_f, title_f, hk._2, hk._3)
        })
          .groupBy(f => f._1).mapValues(f => {
          val abs_f_all: Int = f.map(_._3).sum
          val title_f_all: Int = f.map(_._4).sum
          val keyword_f_all: Int = f.map(_._4).sum
          f.map(tu => {
            //            abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
            (tu._1, tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all, tu._6)
          })
        })
          .flatMap(f => f._2)
          .toSeq //return Seq[(String, String, String, Boolean, Int, Int, Int, Int)] =
      }).flatMap(f => f)
      .filter(f => f._2.keyword == "特殊集合"
      )


  }


  def oldHierarchicalKeyworAnalysis (sourceData : RDD[(HierarchyKeyword)],
                                     corpusMap : Broadcast[Map[Hierarchy, (String, String, String)]]) : RDD[Row] = {
    sourceData.map((pair: (HierarchyKeyword)) => {
      (pair.hierarchy.ApplyID.charAt(0), pair)
    }).groupByKey() // 依照学科代码进行group
      .map(pair => {
        pair._2.toSeq.map(hk => {
          //(abs : title)
          val text = corpusMap.value.getOrElse(hk.hierarchy, ("", "", "")) // 如果因为applyid 修改导致这里取不到 则___
          val keywords = text._3
          val abs = text._1
          val title = text._2
          val keyword = hk.keyword
          val abs_f = StringUtils.countString(abs, keyword)
          val title_f = StringUtils.countString(title, keyword)
          val keyword_f = StringUtils.countString(keywords, keyword)
          // keyword, hierarchy, abstract_f, title_f, keyword_f, isnew
          (keyword, hk, abs_f, title_f, keyword_f, "否")
        })
          .groupBy(f => f._1).mapValues(f => {
          val abs_f_all: Int = f.map(_._3).sum
          val title_f_all: Int = f.map(_._4).sum
          val keyword_f_all: Int = f.map(_._5).sum
          f.map(tu => {
            //            abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
            (tu._1, tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all, tu._6)
          })
        })
          .flatMap(f => f._2)
          .map(f => (
            f._2.hierarchy.ApplyID, // apply id
            f._2.hierarchy.FOS, // ros
            f._9,  // is new word
            f._1,  // keyword
            f._6 + f._7 + f._8, // all keyword's appearance
            f._3 + f._4 + f._5, // this field keyword's appearance
            f._8,  // keyword appearance in keyword filed where in this applyid
            f._5,  // keyword appearance in keyword field where in this ros
            false
          )).toSeq //return Seq[(String, String, String, Boolean, Int, Int, Int, Int)] =
      }).flatMap(f => f)
      .map(tu => Row.fromTuple(tu))
  }
  def keywordFilter (isNewWord : Boolean, weight : Double, percentage : Double, count : Int) : Boolean ={
    if (isNewWord) {
      weight > 2.0 && percentage > 0.7 && count > 2
    } else {
      weight > 6.0 && count > 15 && count < 55 && percentage > 0.5
    }
  }
  def weight (keywordCount : Int, abstractCount : Int, titleCount : Int) : Double = {
    1.0 * keywordCount + 0.2 * abstractCount + 0.4 * titleCount
  }
}
