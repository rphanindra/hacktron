package com.dbs.hacktron.bigdata

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/******
 * Object DataserLoader
 * Description : To load the datasets using the config file
 */
object DatasetLoader {

  val spark = SparkSession.builder.master("local")
    .appName("DBS Hacktron Challenge - DatasetLoader").enableHiveSupport().getOrCreate()

  import spark.implicits._
  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val table_config = "C:\\Users\\welcome\\Desktop\\DBS\\tables_config"
  for (line <- scala.io.Source.fromFile(table_config).getLines()) {
    val tableName = line.slice(0, line.indexOf("|"))
    val header = line.slice(1, line.indexOf("|")).replaceAll(",","|")
    val tableFile = sc.textFile("C:\\Users\\welcome\\Desktop\\DBS\\"+tableName+".txt")
    val tgtFile=tableName+"_temp"
    // To remove splecial characters and get fresh file without special characters
    tableFile.map(x => x.toString.replaceAll("$","")).saveAsTextFile("C:\\Users\\welcome\\Desktop\\DBS\\"+tgtFile+".txt")
    // Load the file to global_temp view
    spark.read.format("csv").option("delimiter","|").load("C:\\Users\\welcome\\Desktop\\DBS\\"+tgtFile+".txt").write.format("orc").mode("overwrite").saveAsTable(tableName)
  }

}
