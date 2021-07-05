package com.practices.exmaple.example1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType,StringType, StructField, StructType}

object ArrayDataType {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("UDFFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
   val data = Seq(Row("school",List("pen","paper","book")))
    val schema = StructType(Array(StructField("name",StringType,false),StructField("items",ArrayType(StringType),false)))
    val df = spark.createDataFrame(sc.parallelize(data),schema)
    df.select($"name",explode($"items")).show()
df.show(false)
       spark.stop()
  }
}