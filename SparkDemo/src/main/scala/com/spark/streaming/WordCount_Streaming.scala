package com.spark.streaming

import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object WordCount_Streaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkStreaming").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val lines = ssc.socketTextStream("ec2-13-232-6-54.ap-south-1.compute.amazonaws.com", 9999)
    lines.flatMap(x =>x.split(" ")).map(x =>(x,1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}