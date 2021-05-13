package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._

object usecaseReduceByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecaseReduceByKey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val data = "C:\\bigdata\\datasets\\donations.txt"
    val rdd = spark.sparkContext.textFile(data)

    val skip = rdd.first()
    val res = rdd.filter(x => x != skip).map(x => x.split(",")).map(x => (x(0), x(1).toInt)).reduceByKey((a, b) => a + b)
      .sortBy(x => x._2, false)
    res.take(9).foreach(println)

    spark.stop()
  }
}