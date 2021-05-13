package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object usecase2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usecase2").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    //File location and type
    val file_location = "C:\\bigdata\\datasets\\asl.csv"
   //in sql 3 imp dml query available
    //select * from tab where age>30;

    val rdd = spark.sparkContext.textFile(file_location)
    val fst = rdd.first()
    val res1 = rdd.filter(x => x!=fst).map(_.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._2>30)

  //  res1.collect.foreach(println)
    //sql ... select city, count(*) cnt from tab group by city order by cnt desc
    // if u want to group by first select what column u want to group followed by , 1 let eg: (x(2),1)
//reduceByKey used to group the values ... based on key .. add/grop the value. rbk proces only value not key.

    val skip = rdd.first()
    val res = rdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((a,b)=>a+b)
      //.sortBy(x=>x._2,false)
    res.take(9).foreach(println)
    spark.stop()
  }
}