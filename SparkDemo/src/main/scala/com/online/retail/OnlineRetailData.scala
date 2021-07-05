package com.online.retail

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OnlineRetailData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("OnlineRetailData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.sql
    import spark.implicits._
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option("mode","failfast")
      .load("E:\\linkedin\\OnlineRetail.csv")
    //schema
    println("============================Schema==============================")
    df.printSchema()
    df.cache() //caching the data to avoid the calculations
    //getting the data according to country
    println("============================country count==============================")
    val dfCountry = df.groupBy("Country").count.sort("Country")
    dfCountry.show(5,false)
  //filter column
    println("============================Filter column==============================")
    val dfFilterColumn = df.select("*").where($"InvoiceNo"==="536759")
    dfFilterColumn.show(5,false)
    println("============================total amount according to invoice==============================")
    val t_amount = udf(totalAmount _)
    val seq = Seq("InvoiceNo","Description","Country")
    val columns = seq.map(x => col(x))
    val df_totalOrder = df.select(t_amount($"UnitPrice",$"Quantity").as("total_amount"),$"InvoiceNo").groupBy(col("InvoiceNo")).sum()
    df_totalOrder.show(5,false)
    println("============================top 10 product in uk==============================")
    val df_10Product_Uk = df.select(t_amount($"UnitPrice",$"Quantity").as("total_amount"),$"InvoiceNo",$"Description",$"Country")
      .groupBy(col("Country"),col("Description"))
      .sum("total_amount")
      .filter($"Country"==="United Kingdom")
      .sort(col("sum(total_amount)").desc)
    df_10Product_Uk.show(5,false)

    spark.stop()
  }
  def totalAmount(amount:Double, qty:Int):Double = amount * qty
}