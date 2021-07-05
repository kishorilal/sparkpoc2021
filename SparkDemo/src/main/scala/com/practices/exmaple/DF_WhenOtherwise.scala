package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DF_WhenOtherwise {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DF_WhenOtherwise").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = List(("James","","Smith","36636","M",60000),
      ("Michael","Rose","","40288","M",70000),
      ("Robert","","Williams","42114","",400000),
      ("Maria","Anne","Jones","39192","F",500000),
      ("Jen","Mary","Brown","","F",0))

    val cols = Seq("first_name","middle_name","last_name","dob","gender","salary")
    val df = spark.createDataFrame(data).toDF(cols:_*)
    df.withColumn("new_gender",when('gender.equalTo("M"),"Male")
      .when($"gender".eqNullSafe("F"),"Female").otherwise("unknow")).show(false)
    //df.show(false)
    val df4 = df.select(col("*"), when(col("gender") === "M","Male")
      .when(col("gender") === "F","Female")
      .otherwise("Unknown").alias("new_gender"))
    df4.show(false)
    df.withColumn("New_gender",expr("case when gender='M' then 'Male'"+
    "when gender='F' then 'Female'"+
    "else 'not defined' end")).show(false)
    df.select(col("*"),expr("case when gender='M' then 'Male'"+
      "when gender='F' then 'Female'"+
      "else 'gender is not defined' end").alias("newgender")).show(false)
    spark.stop()
  }
}