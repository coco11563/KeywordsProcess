package pub.sha0w.xls

import java.io.File

import org.apache.poi.ss.usermodel.{Row, Sheet, Workbook}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, Row => SparkRow}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import pub.sha0w.xls.Object.{Keyword, KeywordSheet}
import pub.sha0w.xls.Utils.XLS

import scala.collection.mutable

object XLSProcess {


  val path_A : String = "src/main/res/A关键词.xls"
  val path_B : String = "src/main/res/B关键词.xls"
  val path_C : String = "src/main/res/C关键词.xls"
  val path_D : String = "src/main/res/D关键词.xls"
  val path_E : String = "src/main/res/E关键词.xls"
  val path_F : String = "src/main/res/F关键词.xls"
  val path_G : String = "src/main/res/G关键词.xls"
  val path_H : String = "src/main/res/H关键词.xls"

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

    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
      .set("hive.metastore.uris", "thrift://packone123:9083")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    hiveContext.setConf("hive.metastore.uris", "thrift://packone123:9083")
    val result_schema = StructType(Array(StructField("APPLYID",StringType, nullable = true),
      StructField("RESEARCH_FIELD",StringType, nullable = true),
      StructField("KEYWORD",StringType, nullable = false)))
    for (pair <- yearMap) {
      val rdd = sc.parallelize(pair._2)
      hiveContext.createDataFrame(rdd.map(k => {
        (k.applyid, k.researchField, k.name)
      }).map(r => SparkRow.fromTuple(r)), result_schema).write.mode(SaveMode.Overwrite)
        .saveAsTable(s"origin.${pair._1}_provided_keyword")
    }
  }
}
