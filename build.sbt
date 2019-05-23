name := "KeywordsProcess"

version := "0.1"

scalaVersion := "2.10.5"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.3",
  "org.apache.spark" % "spark-sql_2.10" % "1.6.3",
  "org.apache.spark" % "spark-hive_2.10" % "1.6.3"
)
