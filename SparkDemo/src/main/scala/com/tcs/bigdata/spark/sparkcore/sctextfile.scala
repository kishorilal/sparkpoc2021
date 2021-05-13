package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sctextfile {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sctextfile").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
// second way create rdd
    val data = "C:\\bigdata\\datasets\\asl.csv"
    val aslrdd = sc.textFile(data) // if u have external data , that data convert to rdd ...  use textFile
    val skip = aslrdd.first()
    val res = aslrdd.filter(x=>x!=skip).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._3=="blr" && x._2>29)
    //select * from tab where city='blr' and age>29;
    res.collect.foreach(println)

    spark.stop()
  }
}