package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object dataframeapi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("dataframeapi").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val df = spark.read
      .format("csv")
      .option("header", "true") // Use first line of all files as header
     .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter",";")
      .option("path",data)
      .load()
    df.show(9)
    df.printSchema()


    spark.stop()
  }
}