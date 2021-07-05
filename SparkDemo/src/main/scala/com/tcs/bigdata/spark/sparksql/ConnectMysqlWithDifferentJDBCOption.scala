package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object ConnectMysqlWithDifferentJDBCOption {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConnectMysqlWithDifferentJDBCOption").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val url = "jdbc:mysql://mysql.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee"
    //setting the property
    val prop = new Properties()
    prop.put("user","admin")
    prop.put("password","admin123")  //error Access denied for user 'admin123'@'171.76.106.215' (using password: YES) either user name or password wrong
    prop.put("driver","com.mysql.jdbc.Driver")
    //getting the table value
    println("========================= simple connection  ==========================")
    val df = spark.read.jdbc(url,"emp",prop)
    df.show(5,false)
    println("========================= simple connection with three argument and no of partiton ==========================")
    val df_3 = spark.read.jdbc(url,"emp","sal",800,1500,5,prop)
   // df_3.show(5,false)
    println("No of partitions == "+df_3.rdd.getNumPartitions)
    println("================Partition value ========================")
    df_3.foreachPartition(f =>{
      println("partition")
      f.foreach(println)
    })
    println("========================= simple connection with predicate ==========================")
    val df_4 = spark.read.jdbc(url,"emp",Array("sal>1600"),prop)
    // df_3.show(5,false)
    println("No of partitions == "+df_4.rdd.getNumPartitions)
    println("================Partition value ========================")
    df_4.foreachPartition(f =>{
      println("partition")
      f.foreach(println)
    })
    spark.stop()
  }
}