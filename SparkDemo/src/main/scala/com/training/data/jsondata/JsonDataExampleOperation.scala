package com.training.data.jsondata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Properties

object JsonDataExampleOperation {
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
    //df.printSchema()
    //df.show(5, false)

    //specifiying the column name
    //val df1 = df.select(col("_id").as("id"),col("city"),col("loc")(1).as("altitude")
    val df1 = df.select(col("_id").as("id"),col("city"),col("loc")(0).as("latitude"),col("loc")(0).as("altitude"),
      col("pop"),col("state"))
    //df1.show(false)
    println("========================group by operation=========================")
    //df.withColumn("state",col("state")).groupBy("state").count.as("citycount").orderBy(desc("state")).show(false)
//    val property = new Properties()
//    property.put("user","admin")
//    property.put("password","admin123")
//    property.put("db","oracle")
//    property.put("table","zipcode")
//    val url = "";
df.withColumn("lati",explode(col("loc"))).show(false)
    //df1.write.mode("append").format("csv").option("header","true").save("E:\\save\\output\\zipdata")
    spark.stop()//
  }
}
