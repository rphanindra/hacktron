package com.dbs.hacktron.bigdata

object DbsDriver {

  def main(args: Array[String]):Unit= args(0) match {

    case "DatasetLoader" => DatasetLoader
    case "QueryRunner" => QueryRunner

  }

}
