package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Foreach {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Foreach").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq((1 ->"java"),(2->"spark"),(3 ->"c++"),(4->"C"),(5 ->"math"),(6->"chemistry")
    ,(7 ->"internet security"),8->("software engineering"),(9 ->"principle programming")
      ,(10->"statistic"),(11 ->"discrete math"),(12->"organic chemistry"),(13 ->"physics"))
    val schema = StructType(Array(StructField("id",IntegerType,false),StructField("name",StringType,true)))
    //val df = spark.createDataFrame(sc.parallelize(data),schema).toDF()
    //val rdd = sc.parallelize(data)
    val df = data.toDF("id","name")
   df.show()
    spark.stop()
  }
}