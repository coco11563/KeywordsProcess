package pub.sha0w.ETL.Statistic

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object KeywordHiarachyCompare {

  val cschema = StructType(Array(StructField("applyid",StringType, nullable = true), StructField("count",IntegerType, nullable = true)))
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val table_1 = hiveContext.read.table("NSFC.compare_result_" + args(0) + "_ex")
    val table_2 = hiveContext.read.table("NSFC.compare_result_" + args(0) + "_in")
    val schema = table_1.schema
    val hiarachyFieldIndex = schema.fieldIndex(args(1))
    val resultRdd = table_1.rdd.map(r => (r.getAs[String](hiarachyFieldIndex), 1))
    //1
    val resultLabel1 = resultRdd.map(pair => (pair._1.substring(0,1) + args(0) + "ex", pair._2)).reduceByKey(_ + _).map(r => Row.fromTuple(r))
    //2
    val resultLabel2 = resultRdd.map(pair => (pair._1.substring(0,3) + args(0) + "ex", pair._2)).reduceByKey(_ + _).map(r => Row.fromTuple(r))

    hiveContext.createDataFrame(resultLabel1,cschema).write.mode(SaveMode.Append).saveAsTable("NSFC.KeywordHiarachyCompare_CODE")

    hiveContext.createDataFrame(resultLabel2,cschema).write.mode(SaveMode.Append).saveAsTable("NSFC.KeywordHiarachyCompare_CODE_NUM")

    val schema2 = table_2.schema
    val hiarachyFieldIndex2 = schema2.fieldIndex(args(1))
    val resultRdd2 = table_2.rdd.map(r => (r.getAs[String](hiarachyFieldIndex2), 1))
    //1
    val result2Label1 = resultRdd2.map(pair => (pair._1.substring(0,1) + args(0) + "in", pair._2)).reduceByKey(_ + _).map(r => Row.fromTuple(r))
    //2
    val result2Label2 = resultRdd2.map(pair => (pair._1.substring(0,3) + args(0) + "in", pair._2)).reduceByKey(_ + _).map(r => Row.fromTuple(r))

    hiveContext.createDataFrame(result2Label1,cschema).write.mode(SaveMode.Append).saveAsTable("NSFC.KeywordHiarachyCompare_CODE")

    hiveContext.createDataFrame(result2Label2,cschema).write.mode(SaveMode.Append).saveAsTable("NSFC.KeywordHiarachyCompare_CODE_NUM")

  }
}
