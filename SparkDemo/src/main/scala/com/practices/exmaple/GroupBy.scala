package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField,IntegerType}

object GroupBy {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DistinctElement").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data = Seq(("Banana",1000,"OH","USA"), ("Carrots",1500,"OH","USA"), ("Beans",1600,"OH","USA"),
      ("Orange",2000,"OH","USA"),("Orange",2000,"OH","USA"),("Banana",400,"OH","China"),
      ("Carrots",1200,"OH","China"),("Beans",1500,"OH","China"),("Orange",4000,"OH","China"),
      ("Banana",2000,"OH","Canada"),("Carrots",2000,"OH","Canada"),("Beans",2000,"OH","Mexico"))

    val df = data.toDF("Product","Amount","State","Country")
    df.show()
    println("============================ group by ================================")
    df.groupBy("Country").agg(mean("Amount"),sum("Amount"),
      count("Country")).show(false)
//    println("contains============"+df.columns.contains("State"))
//    println("contains"+df.schema.fieldNames.contains("State"))
//    println("contains"+df.schema.contains(StructField("Country",StringType,false)))
    spark.stop()
  }
}