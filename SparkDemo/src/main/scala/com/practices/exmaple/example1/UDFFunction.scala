package com.practices.exmaple.example1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object UDFFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("UDFFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val columns = Seq("Seqno","Quote")
    val data = Seq(("1", "Be the change that you wish to see in the world"),
      ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
      ("3", "The purpose of our lives is to be happy.")

    )
    val df = data.toDF(columns:_*)
    df.show(false)
    val convertFunction = udf(convertToFirst)
    df.createOrReplaceTempView("tab")
    df.select($"Seqno",convertFunction(col("Quote")).as("Quote")).show(false)
    spark.udf.register("convertUDF", convertToFirst)
    df.createOrReplaceTempView("QUOTE_TABLE")
    spark.sql("select Seqno, convertUDF(Quote) from QUOTE_TABLE").show(false)
    spark.stop()
  }
  val convertToFirst=(line:String)=>{
    val f = line.split(" ")
    f.map(f => f.substring(0,1).toUpperCase+f.substring(1,f.length)).mkString(" ")
  }
}