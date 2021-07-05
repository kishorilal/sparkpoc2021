package com.online.retail

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OnlineRetailData_e3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("OnlineRetailData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option("mode","failfast")
      .load("E:\\linkedin\\Ex_Files_Apache_Spark_EssT\\Exercise Files\\cogsley_sales.csv")
    //schema
    println("============================Schema==============================")
    df.printSchema()
    df.cache() //caching the data to avoid the calculations
    println("============================ printing the data==============================")
    df.show(5,false)
    println("============================ find the higest slaes company ==============================")
    val df_TotalSales = df.select($"CompanyName",$"SaleAmount")
      .groupBy($"CompanyName")
      .sum("SaleAmount")
     .orderBy(col("CompanyName"),col("sum(SaleAmount)").desc)
    df_TotalSales.show(5,false)
    spark.stop()
  }

}