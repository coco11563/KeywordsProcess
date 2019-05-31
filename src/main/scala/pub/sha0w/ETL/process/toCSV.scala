package pub.sha0w.ETL.process

import java.io.{BufferedReader, File, FileInputStream, FileReader}

import scala.util.parsing.json.JSON

object toCSV {
  def main(args: Array[String]): Unit = {
    val f : File = new File("pub/sha0w/ETL/process/keyword_recommend.json")
    val reader = new BufferedReader(new FileReader(f))
    var str : String = null
    while ({
      str = reader.readLine()
      str != null
    }) {
      val json = JSON.parseFull(str)
      if (json.isDefined) {
        val job = json.get.asInstanceOf[Map[String, Any]]

      }
    }
  }
}
