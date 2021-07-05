package com.practices.exmaple.join

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Union {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Union").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.printSchema()
    df.show()
    val simpleData2 = Seq(("James","Sales","NY",90000,34,10000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df2 = simpleData2.toDF("employee_name","department","state","salary","age","bonus")
    df.union(df2).show(false)
    df.union(df2).distinct().show(false)
    spark.stop()
  }
}