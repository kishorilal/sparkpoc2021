package com.sqlfunction

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StringFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("StringFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq(("jAMES","Peter",35,"Apple","Bangalore"),("James","Peter",35,"Apple","Bangalore"),
      ("jame","Peter",35,"Apple","Bangalore"),("James","Peter",35,"Apple","Bangalore"))
    val df  = spark.createDataFrame(data).toDF("name","lname","ag","company","location")
    //val df1 = df.select(initcap($"name"))
    //val df1 = df.select(concat($"name",$"lname").as("full name"))
    //val df1 = df.select(concat_ws(" ",$"name",$"lname").as("full name"))
    //val df1 = df.select(ascii(col("name")))
    //val df1 = df.select(instr(col("location"),"Gal"))
    //val df1 = df.select(regexp_replace(col("name"),"AMES","ames"))
    //val df1 = df.select(repeat(col("name"),3))
    val df1 = df.select(trim(col("name"),"es"))

    df1.show(false)
    spark.stop()
  }
}