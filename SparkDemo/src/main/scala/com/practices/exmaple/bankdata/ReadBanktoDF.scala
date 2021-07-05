package com.practices.exmaple.bankdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ReadBanktoDF {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ReadBandtoDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
  val df = spark.read
    .format("csv")
    .option("header","true")
    .option("mode","failfast")
    .option("inferSchema","true")
    .option("delimiter",";")
    .load("C:\\bigdata\\datasets\\bank-full.csv")
   df.createOrReplaceTempView("bank")
    val df1 = df.select($"marital")
    val convert = udf(returnValue _)
   // val res = spark.sql("select count(marital) from bank groupby marital")
//    val df2 = df1.select($"marital",(when($"marital"==="married",1) otherwise(0)).alias("All-married"),
//      (when($"marital"==="single",1) otherwise(0)).alias("All-single"),
//      (when($"marital"==="divorced",1) otherwise(0)).alias("All-divorced")
//    )
    //val df2 = df1.select($"marital",convert($"marital").alias("status"))
   // val df3 = df2.groupBy($"marital").agg(sum($"status").alias("status")).orderBy(desc("status"))
    val df4 = df1.groupBy($"marital").agg(count($"marital"))
    df4.show(9)
    spark.stop()
  }
  def returnValue(s: String):Int =s match {
    case "married" |"single" |"divorced" =>1
    case _ =>0
  }
}