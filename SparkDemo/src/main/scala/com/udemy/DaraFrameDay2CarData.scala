package com.udemy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DaraFrameDay2CarData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DaraFrameDay3CarData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
//    val df = spark.read.format("csv")
//      .option("inferSchema","true")
//      .option("mode","failfast")
//      .option("header","true")
//      .load("E:\\Spark_Udemy\\CarPrice_Assignment.csv")

   val df1 = spark.range(1).selectExpr("""'{"Singapur":{"tier":["c1","c2","c3"]}}' as jsoncolumn""")
    val df2 = df1.select(json_tuple(col("jsoncolumn"),"Singapur").alias("extracted_column"))
    df2.select($"*",get_json_object(col("extracted_column"),"$.tier[2]").alias("extracted_column")).show()

    df2.show()
    spark.stop()
  }
}