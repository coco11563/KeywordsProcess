package pub.sha0w.ETL.Statistic

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import pub.sha0w.ETL.KeywordProcessWithBias
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword}
import pub.sha0w.xls.Object.KeywordSheet

object KeywordCompare {
  private val logger: Logger = LoggerFactory.getLogger(KeywordProcessWithBias.getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
//    conf.getAll.foreach(pair => {
//      logger.info(pair._1 + ":" + pair._2 + "\n")
//    })
//    logger.info("输出系统环境变量\n")
//    sys.props.toArray.foreach(pair => {
//      logger.info(pair._1 + ":" + pair._2 + "\n")
//    })
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val table1 = hiveContext.read.table(args(0)) //basic "origin.2018_provided_keyword"
    val table2 = hiveContext.read.table(args(1)) //miner  "origin.2017_provided_keyword"
    val table3 = hiveContext.read.table(args(2)) //compare "middle.m_2017_keyword_recommend"

    val schema2 = table2.schema

//    val table2Set: Set[Int] = table2.rdd.map(r => {(
//      r.getAs[String](schema2.fieldIndex("applyid".toUpperCase)),
//      r.getAs[String](schema2.fieldIndex("research_field".toUpperCase)),
//      r.getAs[String](schema2.fieldIndex("keyword".toUpperCase))
//    )}).map(tuple => {
//      new KeywordSheet(tuple._1, tuple._2, tuple._3).li
//    }).flatMap(a => a).map(k => {(k.applyid, k.researchField, k.name)}).map(r => {
//        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1)).toSimpleHash
//      }).collect().toSet
//
//    logger.info("the miner set's length is : " + table2Set.size)
//    val table2Broadcast = sc.broadcast[Set[Int]](table2Set)
    val schema1 = table1.schema

    val table1Set: Set[Int] = table1.rdd.map(r => {(
      r.getAs[String](schema1.fieldIndex("applyid".toUpperCase)),
      r.getAs[String](schema1.fieldIndex("research_field".toUpperCase)),
      r.getAs[String](schema1.fieldIndex("keyword".toUpperCase))
    )}).map(tuple => {
      new KeywordSheet(tuple._1, tuple._2, tuple._3).li
    }).flatMap(a => a).map(k => {(k.applyid, k.researchField, k.name)}).map(r => {
        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1)).toSimpleHash
      }).collect().toSet

    logger.info("the basic set's length is : " + table1Set.size) //11w


    val table1Broadcast = sc.broadcast[Set[Int]](table1Set)
    val schema3 = table3.schema

    val table3RDD = table3.rdd.map(r => {(
      r.getAs[String](schema3.fieldIndex("applyid")),
      r.getAs[String](schema3.fieldIndex("research_field")),
      r.getAs[String](schema3.fieldIndex("keyword"))
    )}).map(tuple => {
      new KeywordSheet(tuple._1, tuple._2, tuple._3).li
    }).flatMap(a => a).map(k => {(k.applyid, k.researchField, k.name)}).map(r => {
        new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
      })
    val inclusive = table3RDD.filter(h => table1Broadcast.value.contains(h.toSimpleHash)).map(f => {
      (f.hierarchy.ApplyID,f.hierarchy.FOS, f.keyword)
    }).map(t => {
      Row.fromTuple(t)
    })

    val exclusive = table3RDD.filter(h => !table1Broadcast.value.contains(h.toSimpleHash)).map(f => {
      (f.hierarchy.ApplyID,f.hierarchy.FOS, f.keyword)
    }).map(t => {
      Row.fromTuple(t)
    })


    val result_schema = StructType(Array(StructField("applyid",StringType, nullable = true),
      StructField("research_field",StringType, nullable = true),
      StructField("keyword",StringType, nullable = false)))

    hiveContext.createDataFrame(exclusive, result_schema).write.mode(SaveMode.Overwrite).saveAsTable(args(3) + "_ex")

    hiveContext.createDataFrame(inclusive, result_schema).write.mode(SaveMode.Overwrite).saveAsTable(args(3) + "_in")

  }
}
