package pub.sha0w.xls

import java.io.File
import java.util.Properties

import org.apache.poi.ss.usermodel.{Row, Sheet, Workbook}
import org.apache.spark.sql.{SaveMode, SparkSession, Row => SparkRow}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import pub.sha0w.xls.Object.{Keyword, KeywordSheet}
import pub.sha0w.xls.Utils.XLS

import scala.collection.mutable

object XLS2019Process {
  def main(args: Array[String]): Unit = {
    val pathList : Array[File] = Array(new File(args(0)))
    val wbList: Seq[Workbook] = pathList.map(f => XLS.processFile(f))
    var yearLi = new mutable.LinkedList[Keyword]()

    for (wb <- wbList) { // every work book
      val sheet: Sheet = wb.getSheetAt(0)
        val sheetName = sheet.getSheetName
        val firstRowIndex = sheet.getFirstRowNum + 1   //第一行是列名，所以不读
        val lastRowIndex = sheet.getLastRowNum
      for (j <- firstRowIndex until lastRowIndex) {
          val row: Row = sheet.getRow(j)
          var keywords: Seq[String] = for (k <- 0 until 3) yield {
            val cell = row.getCell(k)
            if (cell == null) "" else  cell.getStringCellValue
          }
//        for (keyword <- keywords) {
//          println(keyword)
//        }
//        if (keywords.length < 3) {
//          keywords :+= ""
//        }
        val keywordsSheet : Seq[KeywordSheet] = if (keywords(2) != null) {
          keywords(2).split(",").map(str => new KeywordSheet(keywords(0), keywords(1), str))
        } else {
          Seq()
        }
          //applyid, researchfield, keyword
        for (ks <- keywordsSheet) {
          yearLi ++= ks.li
        }
        }
    }

    val spark = SparkSession.builder
      .getOrCreate()
    val mysqladd = "jdbc:mysql://192.168.3.131:3306/NSFC_KEYWOR_DB"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "Bigdata,1234")
    property.put("driver","com.mysql.cj.jdbc.Driver")

    val result_schema = StructType(Array(StructField("APPLYID",StringType, nullable = true),
      StructField("RESEARCH_FIELD",StringType, nullable = true),
      StructField("KEYWORD",StringType, nullable = false)))
      val rdd = spark.sparkContext.parallelize(yearLi)
      spark.sqlContext.createDataFrame(rdd.map(k => {
        (k.applyid, k.researchField, k.name)
      }).map(r => SparkRow.fromTuple(r)), result_schema).write.mode(SaveMode.Overwrite)
        .jdbc(mysqladd,s"${args(1)}_provided_keyword", property)
  }
}
