package pub.sha0w.xls

import java.io.File
import java.util.Properties

import org.apache.poi.ss.usermodel.{Row, Sheet, Workbook}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession, Row => SparkRow}
import pub.sha0w.xls.Object.{Keyword, KeywordSheet}
import pub.sha0w.xls.Utils.XLS

import scala.collection.mutable

object XLSMysqlProcess {
  def main(args: Array[String]): Unit = {
    val pathList : Array[File] = new File(args(0)).listFiles()
    val wbList: Seq[Workbook] = pathList.map(f => XLS.processFile(f))
    val yearMap = new mutable.HashMap[String, mutable.LinkedList[Keyword]]()

    for (wb <- wbList) { // every work book
      val sheet_num = wb.getNumberOfSheets
      for (i <- 0 until sheet_num) { // every sheet

        val sheet: Sheet = wb.getSheetAt(i)
        val sheetName = sheet.getSheetName
        val firstRowIndex = sheet.getFirstRowNum + 1   //第一行是列名，所以不读
        val lastRowIndex = sheet.getLastRowNum
        for (j <- firstRowIndex until lastRowIndex) {
          val row: Row = sheet.getRow(j)
          val firstCellIndex = row.getFirstCellNum.toInt
          val lastCellIndex = row.getLastCellNum.toInt
          val keywords: Seq[String] = for (k <- firstCellIndex until lastCellIndex) yield {
            val cell = row.getCell(k)
            cell.getStringCellValue
          }
          //applyid, researchfield, keyword
          val keywordsSheet : KeywordSheet = new KeywordSheet(keywords(0), keywords(1), keywords(2))
          if (yearMap.contains(sheetName)) {
            yearMap(sheetName) ++= keywordsSheet.li
          } else {
            yearMap.put(sheetName, new mutable.LinkedList[Keyword]() ++ keywordsSheet.li)
          }
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
    for (pair <- yearMap) {
      val rdd = spark.sparkContext.parallelize(pair._2)
      spark.sqlContext.createDataFrame(rdd.map(k => {
        (k.applyid, k.researchField, k.name)
      }).map(r => SparkRow.fromTuple(r)), result_schema).write.mode(SaveMode.Overwrite)
        .jdbc(mysqladd,s"${pair._1}_provided_keyword", property)
    }
  }
}
