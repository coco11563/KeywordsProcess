package pub.sha0w.ETL

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import pub.sha0w.ETL.Objects.{Hierarchy, HierarchyKeyword, Keyword}

object KeywordsProcess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SpringerProcess")
      .set("spark.driver.maxResultSize","2g")
      .set("hive.metastore.uris", "thrift://packone123:9083")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("hive.metastore.uris", "thrift://packone123:9083")

    val lastYearUsed = hiveContext.read.table(args(0))
    val thisYearUpdated = hiveContext.read.table(args(1))
    val recently = lastYearUsed.rdd.map(R => (R.getAs[String]("applyid"),
      R.getAs[String]("fos"),
      R.getAs[String]("keyword"))).map(r => {
      new HierarchyKeyword(r._3, new Hierarchy(r._2, r._1))
    }).collect().toSet
    val recentlyBroadcast = sc.broadcast[Set[HierarchyKeyword]](recently)
    val recentlyFilter : HierarchyKeyword => Boolean = (a : HierarchyKeyword) => {
      val set = recentlyBroadcast.value.contains(a)
    }

    val tmpUpdateRdd = thisYearUpdated
      .rdd
      .map(r => {
        (r.getAs[String]("keywords"), r.getAs[String]("title"),
        r.getAs[String]("applyid"), r.getAs[String]("fos"),
        r.getAs[String]("abstract"))
      })
    //abs title
    val textMap: Map[Hierarchy, (String, String)] = tmpUpdateRdd.map(line => {
      (new Hierarchy(line._4, line._3), (line._5, line._2))
    }).groupByKey
      .mapValues(f => {
        f.reduce((pair_a, pair_b) => {
          (pair_a._1 + " " + pair_b._1, pair_a._2 + " " + pair_b._2)
        })
      }).collect().toMap

    val textBroadcast = sc.broadcast[Map[Hierarchy, (String, String)]](textMap)

    tmpUpdateRdd.map(f => (new Hierarchy(f._4, f._3), f._1))
      .map(f => f._2.split(args(2))
        .map(str => (str, f._1)))
      .flatMap(f => f)
      .map(f => (f._2, f._1))
      .groupByKey
      .mapValues(strs => {
        val sq = strs.toSeq
        (sq.head,sq.length)
      })
      .map(f => (f._2._1, (f._1,f._2._2)))
      .groupByKey
      .map(p => {
        new Keyword(p._1, p._2)
      }).map(k => {
      k.applyText(textBroadcast.value)
    })
  }

  def weight (keywordCount : Int, abstractCount : Int, titleCount : Int) : Double = {
    1.0 * keywordCount + 0.2 * abstractCount + 0.4 * titleCount
  }
}
