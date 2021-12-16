package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._

import java.util.Properties

object GetTableListFromMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConnectMysql").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val url = "jdbc:mysql://database-1.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee"
    val tables = spark.read.format("jdbc")
      .option("url",url)
      .option("user","musername")
      .option("password","mpassword")
      .option("driver","com.mysql.jdbc.Driver")
      .option("dbtable","information_schema.tables")
      .load()
      .filter("table_schema ='employee'")
      .select("table_name")
      .collect()
      .map(x => x.getString(0))

    val prop = new Properties()
    prop.put("user","musername")
    prop.put("password","mpassword")
    prop.put("driver","com.mysql.jdbc.Driver")
    for(i<- tables){
      val df_table = spark.read.jdbc(url,s"$i",prop)
      df_table.show(5,false)
    }
    spark.stop()
  }
}