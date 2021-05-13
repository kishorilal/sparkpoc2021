package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object RcsvWparquet {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("RcsvWparquet").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
   //reading the csv file
    val df = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("timestampFormat","yyyy-MM-dd'T'HH:mm:ss")
      .option("nullValue","NA")
      .option("mode","failfast")
      .load("E:\\SparkTraning\\survey.csv")
    //writing the csv to parwuent
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("E:\\save\\")
    spark.stop()
  }
}