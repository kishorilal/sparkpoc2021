package com.tcs.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object bankRDDvDF {
  case class bankcc(age:Int, job:String, marital:String, education:String, default1:String, balance:Int, housing:String, loan:String, contact:String, day:String, month:String, duration:String, campaign:String, pdays:String, previous:String, poutcome:String, y:String)

  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bankRDDvsDF").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    import spark.implicits._
    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val brdd = sc.textFile(data)
    val skip = brdd.first()
    val cols = skip.replaceAll("\"","").split(";")

/*
    val res = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";"))
      .map(x=>(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
*/
    //  .filter(x=>x._2=="management" && x._3=="married")
      //above step clean data  afer clean convert to dataframe using toDF()
    //toDF() used 2 scenarios 1) rdd convert to dataframe  2) rename all columns
     // val df = res.toDF("age", "job", "marital", "education", "default", "balance", "housing", "loan", "contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y"
//    val df = res.toDF(cols: _*).drop("day","month","duration","campaign","pdays","previous","poutcome","y")
    val res = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";"))
      .map(x=>bankcc(x(0).toInt,x(1),x(2),x(3),x(4),x(5).toInt,x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13),x(14),x(15),x(16)))
    val df = res.toDF()

    df.show(9)
   // res.take(9).foreach(println)

   /* val res = brdd.filter(x=>x!=skip).map(x=>x.replaceAll("\"","")).map(x=>x.split(";"))
      .map(x=>Row.fromSeq(x))*/

//    res.take(9).foreach(println)

    spark.stop()
  }
}