package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JsonToORC {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JsonToORC").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read
      .format("json")
      .option("mode","failfast")
      .load("E:\\save2\\")
    df.write
      .format("orc")
      .mode("overwrite")
      .save("E:\\savedata\\")
    spark.stop()
  }
}