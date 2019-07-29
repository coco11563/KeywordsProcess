package pub.sha0w.ETL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}
import pub.sha0w.ETL.KeywordsProcess.fieldIndex
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword, Keyword}
import pub.sha0w.ETL.Utils.StringUtils
import pub.sha0w.xls.Object.KeywordSheet

object KeywordProcessWithBias {
  private val logger: Logger = LoggerFactory.getLogger(KeywordProcessWithBias.getClass)
  def main(args: Array[String]): Unit = {
    logger.info("这是一个定制化的组件，仅供处理关键字数据 \n" +
      "为了处理关键字属性，该处理器需要输入6个参数并提供两张规范化的Hive表\n" +
      "最后这个处理器会生成一张名为：middle.m_keyword_recommend的关键字推荐表\n" +
      "并且会将生成的CSV输出到args(4)所标识的位置" +
      "Hive表的Schema如下所示")
    logger.info("table 申请书 : \n" +
      "title_zh\tstring" +
      "\ntitle_en\tstring" +
      "\nkeyword_zh\tstring" +
      "\nkeyword_en\tstring" +
      "\nabstract_zh\tstring" +
      "\nabstract_en\tstring" +
      "\napplyid\tstring" +
      "\nresearch_field\tstring")
    logger.info("table 历年关键字 : \n" +
      "applyid\tstring" +
      "\nresearch_field\tstring" +
      "\nkeyword\tstring")
    logger.info("规范化两张表的组成和字段后，还需要输入8个参数")
    logger.info("参数一：申请书表名\n" +
      "参数二：历年关键字表名\n" +
      "参数三：历年关键字内，关键字keyword字段分隔字符\n" +
      "参数四：今年申请书，关键字keyword_zh字段分隔字符\n" +
      "参数五：输出CSV的位置（HDFS）\n" +
      "参数六：Hive metastore" +
      "参数⑦：处理年份\n" +
      "参数⑧：历史关键字年份\n")
    logger.info("说明结束")
    logger.info("输出spark环境变量\n")
    System.setProperty("hive.metastore.uris", args(5)) //hivemetastore = thrift://packone123:9083
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
    conf.getAll.foreach(pair => {
      logger.info(pair._1 + ":" + pair._2 + "\n")
    })
    logger.info("输出系统环境变量\n")
    sys.props.toArray.foreach(pair => {
      logger.info(pair._1 + ":" + pair._2 + "\n")
    })
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val startTime = System.currentTimeMillis()
    val thisYearUpdated = hiveContext.read.table(args(0)) //origin.2019_application
    val lastYearUsed = hiveContext.read.table(args(1)) //origin.2019_provided_keyword
    val recently_schema = lastYearUsed.schema
    /**
      * should be
      * root
      * |-- APPLYID: string (nullable = true)
      * |-- RESEARCH_FIELD: string (nullable = true)
      * |-- KEYWORD: string (nullable = true)
      */
    recently_schema.printTreeString()
    val theYearBeforeLastYear = hiveContext.read.table(args(7))
    val theyblySchema = theYearBeforeLastYear.schema
    val tyblyRDDSet: Set[HierarchyKeyword] = theYearBeforeLastYear.rdd.map(r => {(
        r.getAs[String](theyblySchema.fieldIndex("applyid")),
      r.getAs[String](theyblySchema.fieldIndex("research_field")),
      r.getAs[String](theyblySchema.fieldIndex("keyword"))
    )}).map(tuple => {
      new KeywordSheet(tuple._1, tuple._2, tuple._3).li
    }).flatMap(a => a)
      .map(k => {(k.applyid, k.researchField, k.name)})
      .map(r => {
        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
      }).collect().toSet

    val tyblyRDDSetBroadcast = sc.broadcast[Set[HierarchyKeyword]](tyblyRDDSet)

    val recently = lastYearUsed.rdd.map(R => (R.getAs[String](fieldIndex(recently_schema,"APPLYID")),
      R.getAs[String](fieldIndex(recently_schema,"research_field".toUpperCase())),
      R.getAs[String](fieldIndex(recently_schema,"keyword".toUpperCase))))
      .map(f => {(f._1, f._2, StringUtils.totalSplit(f._3))}) //args(2) : ","
      .map(f => {f._3.map(a => (f._1, f._2, a))}).flatMap(a => a)
      .map(r => {
        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
      }).collect().toSet
    val recentlyBroadcast = sc.broadcast[Set[HierarchyKeyword]](recently)
    val recentlyFilter : HierarchyKeyword => Boolean = (a : HierarchyKeyword) => {
      !recentlyBroadcast.value.contains(a)
    }
    val thisYearUpdated_schema = thisYearUpdated.schema

    /**
      * should be
      * root
      * |-- TITLE_ZH: string (nullable = true)
      * |-- TITLE_EN: string (nullable = true)
      * |-- KEYWORD_ZH: string (nullable = true)
      * |-- KEYWORD_EN: string (nullable = true)
      * |-- ABSTRACT_ZH: string (nullable = true)
      * |-- ABSTRACT_EN: string (nullable = true)
      * |-- APPLYID: string (nullable = true)
      * |-- RESEARCH_FIELD: string (nullable = true)
      */
    thisYearUpdated_schema.printTreeString()
    val tmpUpdateRdd = thisYearUpdated.rdd.map(r => {
      (r.getAs[String](fieldIndex(thisYearUpdated_schema,"keyword_zh".toUpperCase)), r.getAs[String](fieldIndex(thisYearUpdated_schema,"title_zh".toUpperCase)),
        r.getAs[String](fieldIndex(thisYearUpdated_schema,"applyid".toUpperCase)), r.getAs[String](fieldIndex(thisYearUpdated_schema,"research_field".toUpperCase)),
        r.getAs[String](fieldIndex(thisYearUpdated_schema,"abstract_zh".toUpperCase)))
    }).filter(tu => {
      tu._1 != null
    }) // 244272 枚关键词
    //abs title
    val textMap: Map[Hierarchy, (String, String)] = tmpUpdateRdd.map(line => {
      (new Hierarchy(line._4, line._3), (line._5, line._2))
    }).groupByKey.mapValues(f => {
      f.reduce((pair_a, pair_b) => {
        (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2)
      })
    }).collect().toMap

    val textBroadcast = sc.broadcast[Map[Hierarchy, (String, String)]](textMap)
    //args(3) = ；
    val result_mid_rdd = tmpUpdateRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => StringUtils.totalSplit(f._2)
        .map(str => (str, f._1)))
      .flatMap(f => f)
      .map(f => new HierarchyKeyword(f._1, f._2))
      .groupBy(f => f).map(f => (f._1, f._2.size)).filter(f => {
      recentlyFilter(f._1)
    }).map((f: (HierarchyKeyword, Int)) => {(f._1.hierarchy, (f._1.keyword, f._2, !tyblyRDDSetBroadcast.value.contains(f._1)))})
    println("新关键词总数为 : " + result_mid_rdd.count())

    val result_rdd = result_mid_rdd.groupByKey.map((strs: (Hierarchy, Iterable[(String, Int, Boolean)])) => {
      val sq = strs._2.toSeq
      sq.map(a => (strs._1, a))
    }).flatMap(f => f).map((f: (Hierarchy, (String, Int, Boolean))) => (f._2._1, (f._1, f._2._2, f._2._3)))
      .groupByKey.map((p: (String, Iterable[(Hierarchy, Int, Boolean)])) => {
      new Keyword(p._1, p._2)
    }).map(k => {
      k.applyText(textBroadcast.value)
      k.keywordFilter
    }).map(r => {
      r.print
    }).flatMap(a => a).map(t => Row.fromTuple(t))


    val result_schema = StructType(Array(StructField("applyid",StringType, nullable = true),
      StructField("research_field",StringType, nullable = true),
      StructField("source",StringType, nullable = true),StructField("keyword",StringType, nullable = false),StructField("count",IntegerType, nullable = false),StructField("percentage",DoubleType, nullable = false),StructField("weight",DoubleType, nullable = false)
      ,StructField("title_f",IntegerType, nullable = false),StructField("abstract_f",IntegerType, nullable = false),StructField("keyword_f",IntegerType, nullable = false)))
    val result_df = hiveContext.createDataFrame(result_rdd, result_schema)
    /**
      * root
      * |-- applyid: string (nullable = true)
      * |-- research_field: string (nullable = true)
      * |-- source: string (nullable = true)
      * |-- keyword: string (nullable = false)
      * |-- count: integer (nullable = false)
      * |-- percentage: double (nullable = false)
      * |-- weight: double (nullable = false)
      * |-- title_f: integer (nullable = false)
      * |-- abstract_f: integer (nullable = false)
      * |-- keyword_f: integer (nullable = false)
      */
    result_df.write.mode(SaveMode.Overwrite).saveAsTable(s"middle.m_${args(6)}_keyword_recommend_with_bias")

    println("处理共使用时间：" + (System.currentTimeMillis() - startTime))
//    val result_csv_array = result_rdd.repartition(1).map(r => { (r.getAs[String](0).substring(0, 1) ,
//      r.getAs[String](0).replaceAll(",", " ") +
//        "," + {
//        val ret = r.getAs[String](1)
//        if (ret == null) null else {
//          ret.replaceAll(",", " ")
//        }
//      } +
//        "," + r.getAs[String](2).replaceAll(",", " ") +
//        "," + r.getAs[String](3).replaceAll(",", " ") +
//        "," + r.getAs[Int](4) +
//        "," + r.getAs[Double](5) +
//        "," + r.getAs[Double](6) +
//        "," + r.getAs[Int](7) +
//        "," + r.getAs[Int](8) +
//        "," + r.getAs[Int](9))
//    }).groupByKey().collect()
//    for (arr <- result_csv_array) {
//      val filename = arr._1
//      val value = Array("applyid,research_field,source,keyword,count,percentage,weight,title_f,abstract_f,keyword_f") ++ arr._2
//      sc.parallelize(value, 1).saveAsTextFile(args(4) + filename+ "withbias") //"/out/"
//    }
  }

}
