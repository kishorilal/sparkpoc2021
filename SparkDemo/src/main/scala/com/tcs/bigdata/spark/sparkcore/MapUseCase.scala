package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object MapUseCase {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("MapUseCase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("c:\\bigdata\\datasets\\asl.csv")
    val fst = rdd.first()
    val res = rdd.filter(x => x!=fst).map(_.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._2>30).map(x => x._1 +" "+ x._1.length)
    res.collect().foreach(println)
    rdd.foreachPartition(p => println(p.count(y=>true)))

    //rdd.persist(StorageLevel.DISK_ONLY)

    spark.stop()
  }
}