package com.practices.exmaple

import javassist.bytecode.SignatureAttribute.ArrayType
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object Df_FilterExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("Df_FilterExample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
   /* val arrayStructureData = Seq(
      Row(Row("James","","Smith"),List("Python,Java")),
      Row(Row("Anna","Rose",""),List("Python,Java")),
      Row(Row("Julia","","Williams"),List("Python,Java"))
    )
    val innerStruct = StructType( Array(StructField("f_name",StringType,true),StructField("m_name",StringType,true),
      StructField("l_name",StringType,true)))
    val schema = StructType(Array(

     StructField("fullname",StructType( Array(StructField("f_name",StringType,true),StructField("m_name",StringType,true),
       StructField("l_name",StringType,true))),true),
      StructField("Language",org.apache.spark.sql.types.ArrayType(StringType),true)
    ))
    val df = spark.createDataFrame(sc.parallelize(arrayStructureData),schema)
    val df1 = df.withColumn("f_name",col("fullname.f_name"))
    df1.show()

    */
   val arrayStructureData = Seq(
     Row(Row("James","","Smith"),List("Java","Scala","C++"),"OH","M"),
     Row(Row("Anna","Rose",""),List("Spark","Java","C++"),"NY","F"),
     Row(Row("Julia","","Williams"),List("CSharp","VB"),"OH","F"),
     Row(Row("Maria","Anne","Jones"),List("CSharp","VB"),"NY","M"),
     Row(Row("Jen","Mary","Brown"),List("CSharp","VB"),"NY","M"),
     Row(Row("Mike","Mary","Williams"),List("Python","VB"),"OH","M")
   )
    val schema = new StructType()
      .add("fullname",new StructType()
      .add("fname",StringType,true)
      .add("mname",StringType,true)
      .add("lname",StringType,true),true)
      .add("Language",org.apache.spark.sql.types.ArrayType(StringType),true)
      .add("state",StringType,true)
      .add("gender",StringType,true)
    val df = spark.createDataFrame(sc.parallelize(arrayStructureData),schema)
    df.cache()
    val df1 = df.withColumn("fname",$"fullname.fname").withColumn("mname",col("fullname.mname"))
      .withColumn("lname",df("fullname.lname")).withColumn("flan",'Language(0))
    println("================find the record of OH state====================")
    df1.where('state=== "OH").show(false)
    //different option
    df.where($"state" ==="OH").show(false)
    df.where(col("state") ==="OH").show(false)
    df.where(df("state") ==="OH").show(false)
    //filter function
    println("==========filter function =================")
    df1.filter('state=== "OH").show(false)
    df1.filter("state = 'OH'").show(false)

    df.filter($"state" ==="OH").show(false)
    df.filter(col("state") ==="OH").show(false)
    df.filter(df("state") ==="OH").show(false)
    df.printSchema()
    //multiple conditions
    println("========================Multiple condition in where/filter===============================")
    df.filter('state<=>"NY" && 'Language(1)=!="VB").show
    spark.stop()
  }
}