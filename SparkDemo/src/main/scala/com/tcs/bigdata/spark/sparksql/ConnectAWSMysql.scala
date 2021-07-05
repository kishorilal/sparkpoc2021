package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ConnectAWSMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConnectAWSMysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//    val df = spark.read.format("jdbc")
//      .option("url","jdbc:mysql://mysql.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee?useSSL=false")
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("dbtable","dept")
//      .option("user","admin")
//      .option("password","admin123")
//      .load()
    val df1 = spark.read.format("jdbc")
      //.option("url","jdbc:mysql://mysql.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee?useSSL=false")
      .option("url","jdbc:mysql://database-1.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","emp")
      .option("user","admin")
      .option("password","admin123")
      .load()
  // val df = df1.withColumn("comm");
    df1.show()
    spark.stop()
  }
}