name := "KeywordsProcess"

version := "0.1"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.3",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.3",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.3",
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "8.0.16"
)
// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "4.1.0"
// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "4.1.0"
