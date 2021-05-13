package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkReduceBykeyExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkReduceBykeyExample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\2015.csv"
    val rdd = spark.sparkContext.textFile(data)
    val f_Row = rdd.first()
    val rd1 = rdd.filter(_!=f_Row).map(x => x.split(",")).map(x => (x(1),x(3).toFloat)).reduceByKey((a,b)=>a+b).sortBy(x =>x._2,false)
    //val rd1 = rdd.filter(_!=f_Row).map(x => x.split(",")).map(x => (x(1),x(3).toFloat)).aggregateByKey((a,b) => a+b)
    rd1.collect.foreach(println)
    spark.stop()
  }
}