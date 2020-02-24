package pub.sha0w.ETL

import java.util.Properties

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import pub.sha0w.ETL.KeywordsProcess.fieldIndex
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}
import pub.sha0w.ETL.Utils.StringUtils
import pub.sha0w.xls.Object.KeywordSheet

import scala.collection.mutable

/**
  * /usr/hdp/current/spark2-client/bin/spark-submit
  * --master yarn --deploy-mode
  * client --class pub.sha0w.ETL.KeywordProcessWithMysql
  * --driver-memory 16G --driver-cores 4 --executor-memory 8G --num-executors 8 --executor-cores 4 ~/keywordsprocess_2.11-0.1.jar "origin.o_2018_application" "origin.o_2018_provided_keyword" "2018"
  * "middle.m_2017_keyword_recommend"
  */
object KeywordProcessWithMysql {
  private val logger: Logger = LoggerFactory.getLogger(KeywordProcessWithBiasModified.getClass)

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
    logger.debug(args.toString)

    val oldKeyword = spark.
      read.
      format("jdbc").
      jdbc(mysqladd,args(1), property) //origin.2019_provided_keyword
    val recently_schema = oldKeyword.schema
    val lastYearRecSet: Set[HierarchyKeyword] = if (args(3) != "none") {
      val lastYearRecommand =spark.
        read.
        format("jdbc").
        jdbc(mysqladd,args(3), property)
      val lastYearRecSchema = lastYearRecommand.schema
      //middle.m_recommand_keyword_2018
      //这部分得到去年推荐的关键字数据
      lastYearRecommand.rdd.map(r => {(
        r.getAs[String](lastYearRecSchema.fieldIndex("applyid")),
        r.getAs[String](lastYearRecSchema.fieldIndex("research_field")),
        r.getAs[String](lastYearRecSchema.fieldIndex("keyword"))
      )}).map(tuple => {
        new KeywordSheet(tuple._1, tuple._2, tuple._3).li
      }).flatMap(a => a)
        .map(k => {(k.applyid, k.researchField, k.name)})
        .map(r => {
          new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
        }).collect().toSet
    } else {
      Set()
    }
    val lastYearRecSetBroadcast = spark.sparkContext.broadcast[Set[HierarchyKeyword]](lastYearRecSet)
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
    // 244272 枚关键词
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
    val hierarchyKeywordMap: Map[Hierarchy, List[HierarchyKeyword]] = newAppRdd.map(line => {
      //new hierarchy(ros, applyid) : tuple(abs, title, keyword)
      line._1.split("；").map(str =>  (new Hierarchy(line._4, line._3), str ))
    }).flatMap(f => f).map(pair => {(new HierarchyKeyword(pair._2, pair._1), pair)}).reduceByKey((a, _) => a)
      .values.groupByKey().map(f => {
      (f._1, f._2.toList.map(str => new HierarchyKeyword(str, f._1) ))
    }).collect().toMap

    val combinationHierarchyKeyworMap : mutable.HashMap[Hierarchy, List[HierarchyKeyword]] =
      new mutable.HashMap[Hierarchy, List[HierarchyKeyword]]()
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

    //args(3) = ；
    //f._4 = rs , f._3 = applyid
    // order the keywords with hierarchy structure
    /*  remove by count all
    val result_mid_rdd = newAppRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => StringUtils.totalSplit(f._2) // split the keyword
        .map(str => (str, f._1))) // keyword - hierarchy
      .flatMap(f => f)
      // f => hK(k,h)
      .map(f => new HierarchyKeyword(f._1, f._2))
      .groupBy(f => f).map(f => (f._1, f._2.size)).filter(f => {
      //这个filter让不出现在去年的关键词通过
      !isOldKeyFilter(f._1)
    }).
      // HK, num => {H ,(keyword, num at this research field, isLastYearRecommand)}
      // isLastYearRecommand : false for last year not contain ,
      // isLastYearRecommand : </b>true</b> for last year contain)
      map(
        (f: (HierarchyKeyword, Int)) =>
        {
          // !lastYearRecSetB -> 如果是true 去年未推荐 同时未出现在去年采纳的关键词中 isnewkey
          //                  -> 如果是false 去年推荐 同时未出现在去年采纳的关键词中 isOldKey
          (f._1, f._2, !lastYearRecSetBroadcast.value.contains(f._1))
        }
      )
  */
//    println("新关键词总数为 : " + result_mid_rdd.count())
    // 这部分modified


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
      if (f._1._2) {
        (f._1, f._2, f._3, f._4, f._5, f._6, f._7, if(!lastYearRecSetBroadcast.value.contains(f._1._1)) 1 else -1)
      } else {
        (f._1, f._2, f._3, f._4, f._5, f._6, f._7, 2)
      }
    })


    //modified
    val oldWordCountMap: Map[Hierarchy, Int] = oldKeywordMap.map(f => (f._1, f._2.length))

    val oldWordCountMapBroadcast = spark.sparkContext.broadcast[Map[Hierarchy, Int]](oldWordCountMap)


//    val oldResult: RDD[Row] = oldHierarchicalKeyworAnalysis(oldHierarchy, newAppCorpusMapBroadcast)
    val result = hierarchicalKeyworAnalysisM(allcountCombinationHK, oldWordCountMapBroadcast)
    spark.createDataFrame(result,result_schema)
      .write.
      mode(SaveMode.Overwrite)
      .jdbc(mysqladd,s"m_${args(2)}_keyword_recommend_with_bias", property)
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
//    val result: RDD[Row] = hierarchicalKeyworAnalysis(result_mid_rdd, newAppCorpusMapBroadcast)
//    val result: RDD[Row] = hierarchicalKeyworAnalysis(result_mid_rdd, newAppCorpusMapBroadcast, oldWordCountMapBroadcast)
//    spark.createDataFrame(result,result_schema)
//      .write.
//      mode(SaveMode.Overwrite)
//    .jdbc(mysqladd,s"m_${args(2)}_keyword_recommend_with_bias", property)
//    spark.createDataFrame(oldResult,result_schema).
//      write.
//      mode(SaveMode.Append)
//      .jdbc(mysqladd,s"m_${args(2)}_keyword_recommend_with_bias", property)
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
    StructField("是否需要补充到上级",IntegerType, nullable = false)
  ))
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
  def comboFilter(hCount: Int,
                  allHK: Iterable[((String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Boolean), Double, Int)]):
  Seq[(String, String, String, String, Int, Int, Int, Int, Int)] = {
    val allHKSeq = allHK.toSeq

    val topNumThirtyP = (hCount * 0.3).toInt + 1
    val topNumTenP = (hCount * 0.1).toInt + 1

    val allSortedAllHKSeq : Seq[(Boolean, Double, Int, (String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Boolean))] =
      allHKSeq.map(f => (keywordFilter(
      f._1._9,
      f._3,
      f._2,
      f._1._3 + f._1._4 + f._1._5), f._2, f._3, f._1))
      .sortWith((A, B) => { //排序 优先级：是否合格，权值，频率，字母序
        if (A._1 && B._1) {
          if (A._2 > B._2) true
          else if (A._2 < B._2) false
          else {
            if (A._3 > B._3) {
              true
            } else if (A._3 < B._3){
              false
            } else {
              true
            }
          }
        }
        else if (A._1) true
        else if (B._1) false
        else {
          if (A._2 > B._2) true
          else if (A._2 < B._2) false
          else {
            if (A._3 > B._3) {
              true
            } else if (A._3 < B._3){
              false
            } else {
              if (A._4._1 > B._4._1) true else false
            }
          }
        }
    })

    val qualifiedHKSize = allSortedAllHKSeq.count(f => f._1)
    val chosenHK = if (qualifiedHKSize > topNumThirtyP && hCount != 0) { // 多则删除到30%
      for(i <- 0 until topNumThirtyP withFilter(i => i < allSortedAllHKSeq.size)) yield {
        allSortedAllHKSeq(i)
      }
    } else if (qualifiedHKSize < topNumTenP && hCount != 0) { // 少则增加到10%
      for(i <- 0 until topNumTenP withFilter(i => i < allSortedAllHKSeq.size)) yield {
        allSortedAllHKSeq(i)
      }
    } else { // 无则处理如果无这个词或者不满足之前的filter
      if (hCount == 0) { // 如果hCount为0，则推荐至多五个关键词
        for(i <- 0 until 6 withFilter(i => i < allSortedAllHKSeq.size)) yield {
          allSortedAllHKSeq(i)
        }
      } else {
        allSortedAllHKSeq.filter(f => f._1)
      }
    }

    val chosenHKret = chosenHK.map(f => f._4).map(f => (
      f._2.hierarchy.ApplyID, // apply id
      f._2.hierarchy.FOS, // ros
      if (f._9) "是" else "去年未采纳", // is new word
      f._1,  // keyword
      f._6 + f._7 + f._8, // all keyword's appearance
      f._3 + f._4 + f._5, // this field keyword's appearance
      f._8,  // keyword appearance in keyword filed where in this applyid
      f._5,  // keyword appearance in keyword field where in this ros
      1
    ))
    val upperHK = allSortedAllHKSeq.diff(chosenHK)
      .filter(s => s._1).map(f => f._4).map(f => (
      f._2.hierarchy.ApplyID, // apply id
      null, // ros
      if (f._9) "是" else "去年未采纳", // is new word,  // is new word
      f._1,  // keyword
      f._6 + f._7 + f._8, // all keyword's appearance
      f._3 + f._4 + f._5, // this field keyword's appearance
      f._8,  // keyword appearance in keyword filed where in this applyid
      f._5,  // keyword appearance in keyword field where in this ros
      -1
    ))

    val upperHK_2 = allSortedAllHKSeq.diff(chosenHK)
      .filter(s => !s._1).map(f => f._4).map(f => {
      val all = f._6 + f._7 + f._8
      val this_all = f._3 + f._4 + f._5
      if ((this_all + 0.0) / (all + 0.0) > 0.5 && this_all > 0) {
        (
          f._2.hierarchy.ApplyID, // apply id
          null, // ros
          if (f._9) "是" else "去年未采纳", // is new word
          f._1, // keyword
          f._6 + f._7 + f._8, // all keyword's appearance
          f._3 + f._4 + f._5, // this field keyword's appearance
          f._8, // keyword appearance in keyword filed where in this applyid
          f._5, // keyword appearance in keyword field where in this ros
          // chosen? true for chosen one else
          -1
        )
      } else {
        (
          f._2.hierarchy.ApplyID, // apply id
          f._2.hierarchy.FOS, // ros
          if (f._9) "是" else "去年未采纳", // is new word
          f._1, // keyword
          f._6 + f._7 + f._8, // all keyword's appearance
          f._3 + f._4 + f._5, // this field keyword's appearance
          f._8, // keyword appearance in keyword filed where in this applyid
          f._5, // keyword appearance in keyword field where in this ros
          // chosen? true for chosen one else
          0
        )
      }
    }).filter(s => s._9 != 0)
    chosenHKret union upperHK union upperHK_2
//    ++ upperHK
  }



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
            //  keyword 1, hierarchyKeyword 2, abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8, isnew 9
            (tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all)
          })
        })
          .flatMap(f => f._2)
          .toSeq //return Seq[(String,HierarchyKeyword,Int, Int, Int, Int,Int, Int,Boolean)]
      }).flatMap(f => f)
  }

  def processNewKey(f: ((HierarchyKeyword, Boolean), Int, Int, Int, Int, Int, Int, Int)): (String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Boolean) = {
      //  keyword 1, hierarchyKeyword 2, abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8, isnew 9
      (f._1._1.keyword, f._1._1, f._2, f._3, f._4, f._5, f._6, f._7, if (f._8 == 1) true else false)
  }
  def processOldKey(f: ((HierarchyKeyword, Boolean), Int, Int, Int, Int, Int, Int, Int)): (String, HierarchyKeyword, Int, Int, Int, Int, Int, Int, Int) = {
    (f._1._1.keyword, f._1._1, f._2, f._3, f._4, f._5, f._6, f._7, 2)
  }
  def hierarchicalKeyworAnalysisM(sourceData : RDD[((HierarchyKeyword, Boolean), Int, Int, Int,Int, Int, Int, Int)],  hierachyWordcount : Broadcast[Map[Hierarchy, Int]]) : RDD[Row] = {
    val news = sourceData.filter(f => f._1._2).map(f => {
      processNewKey(f)
    }).map(f => (f._2.hierarchy, (f,weight(f._3,f._4,f._5),
      if ( (f._6 + f._7 + f._8)  == 0) 0 else (f._3 + f._4 + f._5) / (f._6 + f._7 + f._8))))
      .groupByKey.map(f => (hierachyWordcount.value.getOrElse(f._1, 0), f._2))
      .map(pair => comboFilter(pair._1, pair._2))
      .flatMap(f => f)
      //    .filter(r => r._9 != 0)
      .map(tu => Row.fromTuple(tu))

    val old =  sourceData.filter(f => !f._1._2).map(f => {
      processOldKey(f)
    }).map(f => (
      f._2.hierarchy.ApplyID, // apply id
      f._2.hierarchy.FOS, // ros
      "否",  // is new word
      f._1,  // keyword
      f._6 + f._7 + f._8, // all keyword's appearance
      f._3 + f._4 + f._5, // this field keyword's appearance
      f._8,  // keyword appearance in keyword filed where in this applyid
      f._5,  // keyword appearance in keyword field where in this ros
      2
    )).map(tu => Row.fromTuple(tu))

    old union news
  }






}
//  def hierarchicalKeyworAnalysis(sourceData : RDD[(HierarchyKeyword, Int, Boolean)], corpusMap : Broadcast[Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]],
//                                 hierachyWordcount : Broadcast[Map[Hierarchy, Int]]) : RDD[Row] =
//    sourceData.map((pair: (HierarchyKeyword, Int, Boolean)) => {
//    (pair._1.hierarchy.ApplyID.charAt(0), pair)
//  }).groupByKey() // 依照学科代码进行group
//    .map(pair => {
//      pair._2.toSeq.map(hk => {
//        //(abs : title)
//        val text = corpusMap.value(hk._1.hierarchy)
//        val abs = text._1
//        val title: Map[HierarchyKeyword, Int] = text._2
//        val keyword = hk._1.keyword
//        //
//        val abs_f = abs.getOrElse(hk._1, 0)
//        val title_f = title.getOrElse(hk._1, 0)
//        // keyword, hierarchyKeyword, abstract_f, title_f, keyword_f, isnew
//        (keyword, hk._1, abs_f, title_f, hk._2, hk._3)
//      })
//        .groupBy(f =>  f._1).mapValues(f => {
//        val abs_f_all: Int = f.map(_._3).sum
//        val title_f_all: Int = f.map(_._4).sum
//        val keyword_f_all: Int = f.map(_._5).sum
//        f.map(tu => {
//          //  keyword 1, hierarchyKeyword 2, abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8, isnew 9
//          (tu._1, tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all, tu._6)
//        })
//      })
//        .flatMap(f => f._2)
//        .toSeq //return Seq[(String,HierarchyKeyword,Int, Int, Int, Int,Int, Int,Boolean)]
//    }).flatMap(f => f)
//    .map(f => (f._2.hierarchy, (f,weight(f._3,f._4,f._5),
//      if ( (f._6 + f._7 + f._8)  == 0) 0 else (f._3 + f._4 + f._5) / (f._6 + f._7 + f._8))))
//    .groupByKey.map(f => (hierachyWordcount.value.getOrElse(f._1, 0), f._2))
//    .map(pair => comboFilter(pair._1, pair._2))
//    .flatMap(f => f)
////    .filter(r => r._9 != 0)
//    .map(tu => Row.fromTuple(tu))



//  def hierarchicalKeyworAnalysis (sourceData : RDD[(HierarchyKeyword, Int, Boolean)],
//                                  corpusMap : Broadcast[Map[Hierarchy, (Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]]
//                                 ) : RDD[Row] = {
//    sourceData.map((pair: (HierarchyKeyword, Int, Boolean)) => {
//      (pair._1.hierarchy.ApplyID.charAt(0), pair)
//    }).groupByKey() // 依照学科代码进行group
//      .map(pair => {
//        pair._2.toSeq.map(hk => {
//          //(abs : title)
//          val text = corpusMap.value(hk._1.hierarchy)
//          val abs = text._1
//          val title: Map[HierarchyKeyword, Int] = text._2
//          val keyword = hk._1.keyword
//          //
//          val abs_f = abs.getOrElse(hk._1, 0)
//          val title_f = title.getOrElse(hk._1, 0)
//          // keyword, hierarchy, abstract_f, title_f, keyword_f, isnew
//          (keyword, hk._1, abs_f, title_f, hk._2, hk._3)
//        })
//          .groupBy(f =>  f._1).mapValues(f => {
//          val abs_f_all: Int = f.map(_._3).sum
//          val title_f_all: Int = f.map(_._4).sum
//          val keyword_f_all: Int = f.map(_._5).sum
//          f.map(tu => {
//            //            abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
//            (tu._1, tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all, tu._6)
//          })
//        })
//          .flatMap(f => f._2)
//          .toSeq //return Seq[(String, String, String, Boolean, Int, Int, Int, Int)] =
//      }).flatMap(f => f)
//      .map(f => (keywordFilter(
//        f._9,
//        weight(f._3,f._4,f._5),
//        if ( (f._6 + f._7 + f._8)  == 0) 0 else (f._3 + f._4 + f._5) / (f._6 + f._7 + f._8),
//        f._3 + f._4 + f._5), f))
//      .map(f =>
//      {
//        if (f._1) { // 如果过了阈值
//          (
//            f._2._2.hierarchy.ApplyID, // apply id
//            f._2._2.hierarchy.FOS, // ros
//            if (f._2._9) "是" else "去年未采纳", // is new word
//            f._2._1, // keyword
//            f._2._6 + f._2._7 + f._2._8, // all keyword's appearance
//            f._2._3 + f._2._4 + f._2._5, // this field keyword's appearance
//            f._2._8, // keyword appearance in keyword filed where in this applyid
//            f._2._5, // keyword appearance in keyword field where in this ros
//            // chosen? true for chosen one else 1 for choose -1 for to upper 0 for delete
//            1
//          )
//        }
//        else
//        {
//          val all = f._2._6 + f._2._7 + f._2._8
//          val this_all = f._2._3 + f._2._4 + f._2._5
//          if ((this_all + 0.0) / (all + 0.0) > 0.5 && this_all > 0) {
//            (
//              f._2._2.hierarchy.ApplyID, // apply id
//              f._2._2.hierarchy.FOS, // ros
//              if (f._2._9) "是" else "去年未采纳", // is new word
//              f._2._1, // keyword
//              f._2._6 + f._2._7 + f._2._8, // all keyword's appearance
//              f._2._3 + f._2._4 + f._2._5, // this field keyword's appearance
//              f._2._8, // keyword appearance in keyword filed where in this applyid
//              f._2._5, // keyword appearance in keyword field where in this ros
//              // chosen? true for chosen one else
//              -1
//            )
//          } else {
//            (
//              f._2._2.hierarchy.ApplyID, // apply id
//              f._2._2.hierarchy.FOS, // ros
//              if (f._2._9) "是" else "去年未采纳", // is new word
//              f._2._1, // keyword
//              f._2._6 + f._2._7 + f._2._8, // all keyword's appearance
//              f._2._3 + f._2._4 + f._2._5, // this field keyword's appearance
//              f._2._8, // keyword appearance in keyword filed where in this applyid
//              f._2._5, // keyword appearance in keyword field where in this ros
//              // chosen? true for chosen one else
//              0
//            )
//          }
//        }
//      })
//      .filter(r => r._9 != 0)
//      .map(tu => Row.fromTuple(tu))
//  }


//  def oldHierarchicalKeyworAnalysis (sourceData : RDD[HierarchyKeyword],
//                                     corpusMap : Broadcast[Map[Hierarchy,(Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int], Map[HierarchyKeyword, Int])]]) : RDD[Row] = {
//    sourceData.map((pair: HierarchyKeyword) => {
//      (pair.hierarchy.ApplyID.charAt(0), pair)
//    }).groupByKey() // 依照学科代码进行group
//      .map(pair => {
//        pair._2.toSeq.map(hk => {
//          //(abs : title)
//          val text = corpusMap.value.getOrElse(hk.hierarchy, (Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int](), Map[HierarchyKeyword, Int]())) // 如果因为applyid 修改导致这里取不到 则___
////          val text = corpusMap.value(hk.hierarchy)
//          val keywords = text._3
//          val abs = text._1
//          val title = text._2
//          val keyword = hk.keyword
////          val abs_f: Int = abs(hk)
////          val title_f: Int = title(hk)
////          val keyword_f: Int = keywords(hk)
//          val abs_f: Int = abs.getOrElse(hk, 0)
//          val title_f: Int = title.getOrElse(hk, 0)
//          val keyword_f: Int = keywords.getOrElse(hk, 0)
//          // keyword, hierarchy, abstract_f, title_f, keyword_f, isnew
//          (keyword, hk, abs_f, title_f, keyword_f, "否")
//        })
//          .groupBy(f => f._1).mapValues(f => {
//          val abs_f_all: Int = f.map(_._3).sum
//          val title_f_all: Int = f.map(_._4).sum
//          val keyword_f_all: Int = f.map(_._5).sum
//          f.map(tu => {
//            //            abstract_f 3, title_f 4, keyword_f 5, absall 6 , titleall 7, keywordall 8
//            (tu._1, tu._2, tu._3, tu._4, tu._5, abs_f_all, title_f_all, keyword_f_all, tu._6)
//          })
//        })
//          .flatMap(f => f._2)
//          .map(f => (
//            f._2.hierarchy.ApplyID, // apply id
//            f._2.hierarchy.FOS, // ros
//            f._9,  // is new word
//            f._1,  // keyword
//            f._6 + f._7 + f._8, // all keyword's appearance
//            f._3 + f._4 + f._5, // this field keyword's appearance
//            f._8,  // keyword appearance in keyword filed where in this applyid
//            f._5,  // keyword appearance in keyword field where in this ros
//            2
//          )).toSeq //return Seq[(String, String, String, Boolean, Int, Int, Int, Int)] =
//      }).flatMap(f => f)
//      .map(tu => Row.fromTuple(tu))
//  }