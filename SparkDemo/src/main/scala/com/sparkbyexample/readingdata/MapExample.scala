package com.sparkbyexample.readingdata

import org.apache.oro.text.regex.Util
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object MapExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("MapExample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq("Project",
      "Gutenberg’s",
      "Alice’s",
      "Adventures",
      "in",
      "Wonderland",
      "Project",
      "Gutenberg’s",
      "Adventures",
      "in",
      "Wonderland",
      "Project",
      "Gutenberg’s")
    val rdd = sc.parallelize(data)
    //rdd.take(3).foreach(println)
    val structureData = Seq(
      Row("James","","Smith","36636","NewYork",3100),
      Row("Michael","Rose","","40288","California",4300),
      Row("Robert","","Williams","42114","Florida",1400),
      Row("Maria","Anne","Jones","39192","Florida",5500),
      Row("Jen","Mary","Brown","34561","NewYork",3000)
    )
    val structureSchema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("id",StringType)
      .add("location",StringType)
      .add("salary",IntegerType)
    val df = spark.createDataFrame(sc.parallelize(structureData),structureSchema)
    df.show(false)
    val df3 = df
      .map(row=>{
       // val util = new Util()
      val fullName = row.getString(0) +row.getString(1) +row.getString(2)
      (fullName, row.getString(3),row.getInt(5))
    })
   // val df3Map =  df3.toDF("fullName","id","salary")

    //df3Map.printSchema()
   // df3Map.show(false)
   // val combiner = udf(comiber (_:String,_:String,_:String))
//    val df4 = df.mapPartitions(itr => {
////    val res =  itr.map(row =>(row.getString(0),row.getString(1),row.getString(2)))
////     ) res
//   })
//    df4.show(false)
    spark.stop()
  }
  def comiber(fName:String, mName: String, lName: String):String =
    fName+" "+mName+" "+lName
}