package com.dbs.hacktron.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/******
 * Object QueryRunner
 * Description : To run the queries using the config file and save it to temp tables/views for reporting purpose
 */
object QueryRunner {
  val spark = SparkSession.builder.
    master("local")
    .appName("DBS Hacktron Challenge - Query_Runner")
    .enableHiveSupport()
    .getOrCreate()
  import spark.implicits._
  spark.conf.set("spark.debug.maxToStringFields", 10000)
  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  val queryPath="C:\\Users\\welcome\\Desktop\\DBS\\reportQueries"
  for (line <- scala.io.Source.fromFile(queryPath).getLines) {
    val cols = line.split(",").map(_.trim)
    var view_name = cols(0)
    var query = cols(1)
    spark.sql(query).write.format("orc").mode("overwrite").saveAsTable(view_name)

  }

}
