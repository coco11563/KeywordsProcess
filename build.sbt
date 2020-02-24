name := "KeywordsProcess"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.spark" %% "spark-hive" % "2.2.0",
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
  "mysql" % "mysql-connector-java" % "8.0.16"
)
// https://mvnrepository.com/artifact/org.apache.poi/poi
libraryDependencies += "org.apache.poi" % "poi" % "4.1.0"
// https://mvnrepository.com/artifact/org.apache.poi/poi-ooxml
libraryDependencies += "org.apache.poi" % "poi-ooxml" % "4.1.0"

// https://mvnrepository.com/artifact/org.apdplat/word
libraryDependencies += "org.apdplat" % "word" % "1.3.1"

// https://mvnrepository.com/artifact/com.huaban/jieba-analysis
libraryDependencies += "com.huaban" % "jieba-analysis" % "1.0.2"


