package com.dataframe.example

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object LoadBankData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("LoadBankData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
  val data = "C:\\bigdata\\datasets\\us-500.csv";
  val df = spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .option("mode","failfast")
    .option("path",data)
    .load()
  df.createOrReplaceTempView("ustab")
   val fname1 = udf(fullname _)
    spark.udf.register("fname",fname1)
    val df2 = spark.sql("select state,count(*) No_of_records from ustab group by state order by No_of_records desc")
    val df3 = spark.sql("select fname(first_name,last_name) as full_name, * from ustab")

    //example of concat_ws
    val df4 = spark.sql("select concat_ws(' ',first_name,last_name) as full_name, * from ustab")
    val l = df.withColumn("Cname",$"company_name")
    //programming sql
    l.show(100,false)

    spark.stop()
  }
  def fullname(s:String,s1: String):String =
    s+" "+s1
}