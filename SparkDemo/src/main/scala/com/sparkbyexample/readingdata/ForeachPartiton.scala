package com.sparkbyexample.readingdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ForeachPartiton{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().appName("map").master("local[*]").getOrCreate()
    val arr = Array(1,2,3,4,5,6,7,8,9)
    val rdd = sc.parallelize(arr,4)
    rdd.foreachPartition(f =>{
      println("map parttions")
      f.foreach(println)
    })
    println("=====================")
    val rdd1 = rdd.repartition(3)
    rdd1.foreachPartition(f =>{
      println("map parttions")
      f.foreach(println)
    })
    println("=====================")

   


  }
}
