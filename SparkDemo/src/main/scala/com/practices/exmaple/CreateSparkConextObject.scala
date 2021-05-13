package com.practices.exmaple

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object CreateSparkConextObject {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Creating the spark conext").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val array = Array(1,2,3,4,5,6)
    val rdd = sc.parallelize(array,5)
    rdd.foreachPartition(x =>println("rdd  "+x.count(y=> true)))
  }
}