package com.sparkbyexample.readingdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadMultipleFiles {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReadMultipleFiles").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //exmaple reading all the text file from a directory
//    val rdd = sc.textFile("E:\\SparkTraning\\multipleFile\\*")
//    val rdd1 = rdd.flatMap(x=>x.split(" ")).map(x=>(x,1)).reduceByKey((a,b)=> a+b).sortByKey(false)
//    rdd1.collect().foreach(println)

    //example reading all the text file from a directory using wholeTextFiles method
//    val rdd = sc.wholeTextFiles("E:\\SparkTraning\\multipleFile\\*")
//    rdd.collect().foreach(println)
    //pattern matching
//    val rdd = sc.textFile("E:\\SparkTraning\\multipleFile\\*.txt")
//    rdd.collect().foreach(println)
  val rdd = sc.wholeTextFiles("E:\\SparkTraning\\multipleFile\\s*.csv")
    val rdd1 = rdd.mapPartitionsWithIndex{
      (idx,iter) => if(idx == 0) iter.drop(1) else iter
    }
    rdd1.collect().foreach(println)
    spark.stop()
  }
}