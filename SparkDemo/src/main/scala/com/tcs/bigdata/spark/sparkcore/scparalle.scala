package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object scparalle {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val nums = Array(33,4,22,11,9,45,32,12,9)
    // its array its java/scala/jvm objects
  //convert nums to rdd
    val nrdd = sc.parallelize(nums)
    val res =  nrdd.map(x=>x*x).filter(x=>x<200)
    res.collect.foreach(println)

    spark.stop()
  }
}