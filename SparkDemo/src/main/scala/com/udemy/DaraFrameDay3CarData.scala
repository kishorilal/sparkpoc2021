package com.udemy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType,StringType}

object DaraFrameDay3CarData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DaraFrameDay3CarData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read.format("csv")
      .option("inferSchema","true")
      .option("mode","failfast")
      .option("header","true")
      .load("E:\\Spark_Udemy\\CarPrice_Assignment.csv")
    df.printSchema()
   // df.show(4,false)
    df.cache()
    val avg = udf(avergeMpg _)
    df.createOrReplaceTempView("carData")
   // spark.sql("select CarName from carData where price>=15000").show(4,false)
    //df.select($"CarName",$"price").where($"price">=30000).show(4,false)
    //df.withColumn("CarName",$"CarName").where($"CarName" ==="bmd x3").groupBy("CarName").agg(avg("citympg")).show()
   // val df1 = df.withColumn("average_speed",round(avg($"citympg",$"highwaympg")))
    //df1.orderBy($"average_speed".desc).show(4)
    //df1.describe("average_speed").show()
    //df.select(regexp_extract(col("CarName"),"alfa",0).alias("extracted"),$"CarName").show()
    val arrayDF = df.select(split(col("CarName")," ").alias("newArrayCol"))
    //arrayDF.printSchema()
    //val df1 = arrayDF.select(expr("newArrayCol[1]"))

    //val schema = StructType(Array(StructField("CarName",StringType,true),StructField("carbody",StringType,true)))
    //val df1 = df.select(array($"CarName",$"carbody").as("newarray").cast(schema))
   // val df1 = arrayDF.select(size(expr("newArrayCol")))

    val df1 = df.select(split(col("CarName")," ").alias("carnamevalue"),$"carbody")
   // val df2 = df.withColumn("carbody",$"carbody").where($"carbody".like("__dan%") && $"fuelsystem"==="2bbl")
    df1.cache()
    val df2 = df1.select(explode(col("carnamevalue")),$"carbody")
    df2.show(5)
    val df3 = df1.select(explode_outer(col("carnamevalue")),$"carbody")
    df3.show(5)
    val df4 = df1.select(posexplode(col("carnamevalue")),$"carbody")
    df4.show(5)
    val df5 = df1.select(posexplode_outer(col("carnamevalue")),$"carbody")
    df5.show(5)
//    val df5 = df1.select(arr(col("carnamevalue")),$"carbody")
//    df5.show(5)
    spark.stop()
  }

  def avergeMpg(a: Int,b:Int):Int =
    (a + b)/2
}