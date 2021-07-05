package com.training.data.jsondata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}

object JsonDataExampleWithDifferentSelect {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    //defining the schema
    val schema = new StructType(Array(StructField("_id",StringType,true),StructField("city",StringType,false),
    StructField("loc",ArrayType(DoubleType),true),StructField("pop",IntegerType,false),
      StructField("state",StringType,true)))
    //data path
    val data = "C:\\bigdata\\datasets\\zips.json"
    //defining the map
    val map = Map("mode"->"failfast","inferSchema"->"true")
    //reading the json data
    val df  = spark.read.format("json")
      .options(map)
      .schema(schema)
      .load(data)
    df.printSchema()
    df.show(5, false)
    //data cleaning
    //1. removing the _ from id and separating the array data
      df.createOrReplaceTempView("tab")
    println("================== clean data ========================")
    spark.sql("""select _id id,city,loc[0] as altitude,loc[1] logitite,pop,state from tab""").show(5,false)
    println("========================select statement1=========================")
    val column =Array("city","state")
    //(column.map(x => col(x)):_*)
    //select all column name
    df.select("*").show(5,false)

    //specifiying the column name
    println("========================select statement2=========================")
    df.select("_id","city","loc","pop","state").show(5,false)
    println("========================select statement3=========================")
    df.select(col("_id"),col("city"),col("loc"),col("pop"),col("state")).show(5,false)
    println("========================select statement4=========================")
    df.select("_id","city","loc","pop","state").show(5,truncate=false)
    println("========================select statement5=========================")
    val columnNames= df.columns.map(x=>col(x))
    df.select(columnNames:_*).show(5,truncate=false)
    println("========================select statement6=========================")
    //val aa = column.map(x=>col(x))
   // df.withColumn("kl",$"city").show(5,false)
    spark.stop()//
  }
}
