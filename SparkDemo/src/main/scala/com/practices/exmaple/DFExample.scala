package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DFExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("LoadCSV").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._
    import spark.sql
    //val rddCSV = spark.read.options(Map("header"->"true","inferSchema"->"true","mode"->"failfast")).csv("C:\\bigdata\\datasets\\asl.csv")
    val df = spark.read.options(Map("header"->"true","inferSchema"->"true","mode"->"failfast","timestampFormat"->"yyyy-MM-ss'T'HH:mm:ss")).csv("E:\\SparkTraning\\survey.csv")
   // df.select("Timestamp","Age","remote_work","leave").filter("Age>30").show
   val df1 = df.select($"Gender",$"treatment")
    val parseGenderUDF = udf(paraserGenderFunction _)
    val df2 = df1.select($"Gender",(when($"treatment" === "Yes",1).otherwise(0)).alias("All-Yes"),(when($"treatment" === "No",1).otherwise(0)).alias("All-No"))
    val df3 = df2.select((parseGenderUDF($"Gender")).alias("Gender"),$"All-Yes",$"All-No")
    df.write
      .format("parquet")
      .mode("overwrite")
      .save("E:\\savedata\\")

    //  val df4 = df3.groupBy("Gender").agg(sum($"All-Yes"),sum($"All-No")).show()
    // rddCSV.take(3).foreach(println)
    spark.stop()
  }
  def paraserGenderFunction(g: String)={
    g.toLowerCase match {
      case "male" | "m" | "male-ish" | "maile" |"mal" |"male (cis)" | "male "|"make" |"man" |"msle" | "mail" |"malr" |"cis man" |"cis male" => "Male"
      case  "cis female" | "f" |"female" |"woman" |"femake"|"female "| "cis-female/femme"|"female (cis)" |"femail" =>"Female"
      case _=>"Transergender"
    }
  }
}