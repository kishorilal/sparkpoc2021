package com.udemy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object DaraFrameDay5CarData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DaraFrameDay3CarData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.read.format("csv")
          .option("inferSchema","true")
          .option("mode","failfast")
          .option("header","true")
          .load("E:\\Spark_Udemy\\CarPrice_Assignment.csv")

    val df1 = df.select(col("CarName")).count()
    println(df1)
    //df1.show()
    spark.stop()
  }
}