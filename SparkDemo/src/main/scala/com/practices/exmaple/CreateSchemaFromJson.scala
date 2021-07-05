package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.mortbay.util.ajax.JSON.Source

object CreateSchemaFromJson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CreateSchemaFromJson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq(Row(Row("Ram","saini"),"10-10-15","M"),
      Row(Row("kavita","yadav"),"10-4-1992","F"))
    val url = ClassLoader.getSystemResource("scheam.json")//SparkDemo/resource/scheam.json

    println(url)
    spark.stop()
  }
}