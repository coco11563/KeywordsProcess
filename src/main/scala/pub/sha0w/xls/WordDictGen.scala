package pub.sha0w.xls

import java.io.{BufferedOutputStream, File, FileOutputStream, FileWriter}
import java.sql.ResultSet
import java.util.Properties

import com.mysql.cj.conf.HostInfo
import com.mysql.cj.jdbc.{ConnectionImpl, JdbcConnection}

import scala.collection.mutable

object WordDictGen {
  def main(args: Array[String]): Unit = {

    val optPath :String = "src/main/res"
    val mysqladd = "10.0.202.18"
    Class.forName("com.mysql.cj.jdbc.Driver")
    val property = new Properties
    property.put("user","root")
    property.put("password", "")
    property.put("driver","com.mysql.cj.jdbc.Driver")
    val conn: ConnectionImpl = new ConnectionImpl(
      new HostInfo(null, mysqladd,3306,"root", ""))
    val activeConn: JdbcConnection = conn.getActiveMySQLConnection
    processYearDict("2019",activeConn, optPath)
    processYearDict("2018",activeConn, optPath)
    processYearDict("2017",activeConn, optPath)
  }

  def processYearDict(year : String, activeConn : JdbcConnection, outputPath : String) : Unit  = {
    val sql = s"select KEYWORD_ZH from keyword_applicaiton.${year}_APPLICATION_NEW where KEYWORD_ZH is not null"
    println(sql)
    val set : mutable.HashSet[String] = new mutable.HashSet[String]()
    val ps = activeConn.prepareStatement(sql)
    val rs: ResultSet = ps.executeQuery()
    while (rs.next()) {
      val keywords : String = rs.getString(1)
      val splitedKeyword = keywords.split("；")
      for (str <- splitedKeyword) {
        set.add(str)
      }
    }
    rs.close()
    ps.close()
    val filePath : File = new File(outputPath)
    if(!filePath.exists()) filePath.mkdirs()
    val file = new File(outputPath + "/" + s"${year}_dict.txt")
    if (file.exists()) file.delete()
    val fw : FileWriter = new FileWriter(file)
    for (key <- set) {
      if (key != "") {
        fw.append(key)
        fw.append("\r\n")
      }
    }
    fw.flush()
    fw.close()
  }
}
