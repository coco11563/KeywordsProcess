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

    val lastYearUsed = hiveContext.read.table(args(0))
    val thisYearUpdated = hiveContext.read.table(args(1))
    val recently_schema = lastYearUsed.schema
    recently_schema.printTreeString()
    val recently = lastYearUsed.rdd.map(R => (R.getAs[String](recently_schema.fieldIndex("APPLYID")),
      R.getAs[String](recently_schema.fieldIndex("research_field".toUpperCase())),
      R.getAs[String](recently_schema.fieldIndex("keyword".toUpperCase))))
      .map(f => {
        (f._1, f._2, f._3.split(args(2)))
      })
      .map(f => {
        f._3.map(a => (f._1, f._2, a))
      }).flatMap(a => a)
      .map(r => {
      new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
    }).collect().toSet
    val recentlyBroadcast = sc.broadcast[Set[HierarchyKeyword]](recently)
    val recentlyFilter : HierarchyKeyword => Boolean = (a : HierarchyKeyword) => {
      recentlyBroadcast.value.contains(a)
    }
    val thisYearUpdated_schema = thisYearUpdated.schema
    thisYearUpdated_schema.printTreeString()
    val tmpUpdateRdd = thisYearUpdated
      .rdd
      .map(r => {
        (r.getAs[String](thisYearUpdated_schema.fieldIndex("keyword_zh".toUpperCase)), r.getAs[String](thisYearUpdated_schema.fieldIndex("title_zh".toUpperCase)),
        r.getAs[String](thisYearUpdated_schema.fieldIndex("applyid".toUpperCase)), r.getAs[String](thisYearUpdated_schema.fieldIndex("research_field".toUpperCase)),
        r.getAs[String](thisYearUpdated_schema.fieldIndex("abstract_zh".toUpperCase)))
      }).filter(tu => {
      tu._1 != null
    })
    //abs title
    val textMap: Map[Hierarchy, (String, String)] = tmpUpdateRdd.map(line => {
      (new Hierarchy(line._4, line._3), (line._5, line._2))
    }).groupByKey
      .mapValues(f => {
        f.reduce((pair_a, pair_b) => {
          (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2)
        })
      }).collect().toMap

    val textBroadcast = sc.broadcast[Map[Hierarchy, (String, String)]](textMap)

    val result_mid_rdd = tmpUpdateRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => f._2.split(args(3))
        .map(str => (str, f._1)))
      .flatMap(f => f)
      .map(f => (f._2, f._1))
      .filter(f => {
        recentlyFilter(new HierarchyKeyword(f._2, f._1))
      })
    println("新关键词总数为 : " + result_mid_rdd.count())
    val result_rdd = result_mid_rdd
      .groupByKey
      .mapValues(strs => {
        val sq = strs.toSeq
        (sq.head,sq.length)
      })
      .map(f => (f._2._1, (f._1,f._2._2)))
      .groupByKey
      .map(p => {
        new Keyword(p._1, p._2)
      })
      .map(k => {
      k.applyText(textBroadcast.value)
      k.keywordFilter
    })
      .map(r => {
        r.print
      }).flatMap(a => a)
      .map(t => Row.fromTuple(t))
    val result_schema = StructType(Array(StructField("applyid",StringType, nullable = true),
      StructField("research_field",StringType, nullable = true),
      StructField("source",StringType, nullable = true),StructField("keyword",StringType, nullable = false),StructField("count",IntegerType, nullable = false),StructField("percentage",DoubleType, nullable = false),StructField("weight",DoubleType, nullable = false)
      ,StructField("title_f",IntegerType, nullable = false),StructField("abstract_f",IntegerType, nullable = false),StructField("keyword_f",IntegerType, nullable = false)))
    hiveContext.createDataFrame(result_rdd, result_schema).write.mode(SaveMode.Overwrite).saveAsTable("middle.m_keyword_recommend")
  }


}
