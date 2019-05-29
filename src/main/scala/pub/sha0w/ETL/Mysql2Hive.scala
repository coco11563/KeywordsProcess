package pub.sha0w.ETL

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object Mysql2Hive {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
      .set("hive.metastore.uris", "thrift://packone123:9083")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    hiveContext.setConf("hive.metastore.uris", "thrift://packone123:9083")
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "Bigdata,1234")
    property.put("driver","com.mysql.cj.jdbc.Driver")
    val mysql = sqlContext.read.format("jdbc").jdbc("jdbc:mysql://192.168.3.131:3306/NSFC_KEYWOR_DB","2017_APPLICATION_NEW", property)
    hiveContext.createDataFrame(mysql.rdd, mysql.schema).write.format("orc").saveAsTable("origin.2017_application")
  }
}
