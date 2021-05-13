package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ORCtoXML {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ORCtoXML").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val df = spark.read
      .format("orc")
      .option("mode","failfast")
      .load("E:\\savedata\\")
    df.write
      .format("com.databricks.spark.xml")
      .mode("overwrite")
      .save("E:\\save2\\")
    spark.stop()
  }
}