package com.practices.exmaple.example1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NestedData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("UDFFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val structureData = Seq(
      Row(Row("James ","","Smith"),Row(Row("CA","Los Angles"),Row("CA","Sandiago"))),
      Row(Row("Michael ","Rose",""),Row(Row("NY","New York"),Row("NJ","Newark"))),
      Row(Row("Robert ","","Williams"),Row(Row("DE","Newark"),Row("CA","Las Vegas"))),
      Row(Row("Maria ","Anne","Jones"),Row(Row("PA","Harrisburg"),Row("CA","Sandiago"))),
      Row(Row("Jen","Mary","Brown"),Row(Row("CA","Los Angles"),Row("NJ","Newark")))
    )

    val structureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("address",new StructType()
        .add("current",new StructType()
          .add("state",StringType)
          .add("city",StringType))
        .add("previous",new StructType()
          .add("state",StringType)
          .add("city",StringType)))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(structureData),structureSchema)
    //df.printSchema()
    val df3 = df.select(flattenStructSchema(df.schema):_*)
  //  df3.printSchema()
  //  df3.show(false)
   // println(df.schema)

    val arrayArrayData = Seq(
      Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
      Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
      Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
    )

    val arrayArraySchema = new StructType().add("name",StringType)
      .add("subjects",ArrayType(ArrayType(StringType)))

    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
    df1.printSchema()
   val df2 =  df1.select($"name",flatten($"subjects").as("level1"))//.show(false)
    df2.show(false)

       spark.stop()
  }
  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName).as(columnName.replace(".","_")))
      }
    })
  }
}