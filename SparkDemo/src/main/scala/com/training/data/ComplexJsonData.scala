package com.training.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//
object ComplexJsonData {
  def main(args: Array[String]): Unit = {
  print("hello world")
    val spark = SparkSession.builder.master("local[*]").appName("ComplexJsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //val data = "C:\\bigdata\\datasets\\zips.json" // hardcode not recommended in prod env
    val data = args(0) // ercommended
    val df = spark.read.format("json").load(data)
    df.show(6)

    spark.stop()

  }
}