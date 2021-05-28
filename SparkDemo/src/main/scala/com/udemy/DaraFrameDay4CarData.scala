package com.udemy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DaraFrameDay4CarData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DaraFrameDay3CarData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
//example of join
    val df = Map(1 ->"jhon",2->"smith",3 ->"peter").toSeq.toDF("rollNo","name")
    val df1 = Seq((1 ->"math"),(2->"C++"),(2->"python"),(4 ->"java"),(5->"spark"),(6->"hadoop")).toDF("rollNo","subject")
    //inner join
    val joinColumn = df.col("rollNo") ===df1.col("rollNo")
    val joinOn = df.join(df1,joinColumn,"full_outer")
    joinOn.show()
    spark.stop()
  }
}