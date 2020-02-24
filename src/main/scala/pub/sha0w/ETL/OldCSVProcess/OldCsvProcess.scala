package pub.sha0w.ETL.OldCSVProcess

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

class OldCsvProcess {

}
object OldCsvProcess {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .getOrCreate()
    val mysqladd = "jdbc:mysql://10.0.202.18:3306/application_processed"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "")
    property.put("driver","com.mysql.cj.jdbc.Driver")

    val base_file = "/root/old_application"
    //    subject_code1	zh_title	en_title	ckeyword	csummary
    val csv_schema = StructType(Array(
      StructField("applyid",StringType, nullable = false),
      StructField("zh_title",StringType, nullable = false),
      StructField("en_title",StringType, nullable = true),
      StructField("ckeyword",StringType, nullable = false),
      StructField("abstract",StringType, nullable = false)
    ))
    for (i <- (args(0).toInt) to args(1).toInt) {
      val src = Source.fromFile(base_file + "/" + i)
      val iter: Iterator[Array[String]] = src.getLines().drop(1).map(_.split("\t"))
      val rows: Iterator[Row] = iter.filter(f => f(0) != null && !f(0).isEmpty).filter(f => f(4) != null && !f(4).isEmpty).map(f => f.map(str => str.trim)).map(f => Row.fromSeq(f))
      val rdds: RDD[Row] = spark.sparkContext.parallelize(rows.toSeq)
      val basic_etl = spark.sqlContext.createDataFrame(rdds, csv_schema)
      basic_etl.write.mode(SaveMode.Overwrite).jdbc(mysqladd, i + "_APPLICATION_OLD", property)
      print(i)
    }
  }

}
