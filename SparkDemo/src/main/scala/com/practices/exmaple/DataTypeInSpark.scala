package com.practices.exmaple

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataTypeInSpark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataTypeInSpark").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val arrayType = ArrayType(StringType,true)
    println("json "+arrayType.json)
    println("json "+arrayType.prettyJson)
    println("string type "+arrayType.simpleString)
    println("sql type "+arrayType.sql)
    println(" type "+arrayType.containsNull)
    println(" type "+arrayType.elementType)
    println(" type "+arrayType.catalogString)
    println(" type "+arrayType.defaultSize)







    spark.stop()
  }
}