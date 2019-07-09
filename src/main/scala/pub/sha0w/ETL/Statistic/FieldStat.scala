package pub.sha0w.ETL.Statistic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object FieldStat {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    for (str <- args) {
      val tableNameEX = "compare_result_" + str + "_ex"
      val tableNameIN = "compare_result_" + str + "_in"

    }
  }
}
