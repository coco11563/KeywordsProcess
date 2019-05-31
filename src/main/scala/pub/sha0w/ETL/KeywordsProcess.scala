package pub.sha0w.ETL

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword, Keyword}

object KeywordsProcess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val lastYearUsed = hiveContext.read.table(args(0)) //origin.2019_provided_keyword
    val thisYearUpdated = hiveContext.read.table(args(1)) //origin.2019_application
    val recently_schema = lastYearUsed.schema
    /**
      * should be
      * root
      * |-- APPLYID: string (nullable = true)
      * |-- RESEARCH_FIELD: string (nullable = true)
      * |-- KEYWORD: string (nullable = true)
      */
    recently_schema.printTreeString()

    val recently = lastYearUsed.rdd.map(R => (R.getAs[String](recently_schema.fieldIndex("APPLYID")),
      R.getAs[String](recently_schema.fieldIndex("research_field".toUpperCase())),
      R.getAs[String](recently_schema.fieldIndex("keyword".toUpperCase))))
      .map(f => {(f._1, f._2, f._3.split(args(2)))}) //args(2) : ","
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
        (r.getAs[String](thisYearUpdated_schema.fieldIndex("keyword_zh".toUpperCase)), r.getAs[String](thisYearUpdated_schema.fieldIndex("title_zh".toUpperCase)),
        r.getAs[String](thisYearUpdated_schema.fieldIndex("applyid".toUpperCase)), r.getAs[String](thisYearUpdated_schema.fieldIndex("research_field".toUpperCase)),
        r.getAs[String](thisYearUpdated_schema.fieldIndex("abstract_zh".toUpperCase)))
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
    val result_mid_rdd = tmpUpdateRdd.map(f => (new Hierarchy(f._4, f._3), f._1)).map(f => f._2.split(args(3)).map(str => (str, f._1))).flatMap(f => f).map(f => (f._2, f._1)).filter(f => {
        recentlyFilter(new HierarchyKeyword(f._2, f._1))
      })
    println("新关键词总数为 : " + result_mid_rdd.count())

    val result_rdd = result_mid_rdd.groupByKey.mapValues(strs => {
        val sq = strs.toSeq
        (sq.head,sq.length)
      }).map(f => (f._2._1, (f._1,f._2._2))).groupByKey.map(p => {
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
    result_df.write.mode(SaveMode.Overwrite).saveAsTable("middle.m_keyword_recommend")
    val result_csv_array = result_rdd.repartition(1).map(r => { (r.getAs[String](0).substring(0, 1) ,
      r.getAs[String](0).replaceAll(",", " ") +
      "," + {
        val ret = r.getAs[String](1)
        if (ret == null) null else {
          ret.replaceAll(",", " ")
        }
      } +
      "," + r.getAs[String](2).replaceAll(",", " ") +
      "," + r.getAs[String](3).replaceAll(",", " ") +
      "," + r.getAs[Int](4) +
      "," + r.getAs[Double](5) +
      "," + r.getAs[Double](6) +
      "," + r.getAs[Int](7) +
      "," + r.getAs[Int](8) +
      "," + r.getAs[Int](9))
    }).groupByKey().collect()
    for (arr <- result_csv_array) {
      val filename = arr._1
      val value = Array("applyid,research_field,source,keyword,count,percentage,weight,title_f,abstract_f,keyword_f") ++ arr._2
      sc.parallelize(value, 1).saveAsTextFile("/out/" + filename)
    }
  }


}
