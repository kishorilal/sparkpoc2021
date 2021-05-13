package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ParquetToJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ParquetToJson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read
      .format("parquet")
      .option("mode","failfast")
      .load("E:\\savedata\\")
    df.write
      .format("json")
      .mode("overwrite")
      .save("E:\\save2\\")
    spark.stop()
  }
}