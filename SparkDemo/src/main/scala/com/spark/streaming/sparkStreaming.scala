package com.spark.streaming

import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

object sparkStreaming {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkStreaming").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val lines = ssc.socketTextStream("ec2-13-232-6-54.ap-south-1.compute.amazonaws.com", 9999)
    // ur not using anywhere this socket text stream just its for knowledge.
    //lines.print()
    lines.foreachRDD { x => //x is rdd
      val murl = "jdbc:mysql://mysqldb.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/mysqldb?autoReconnect=true&useSSL=false"
      val mprop = new Properties()
      mprop.setProperty("user", "musername")
      mprop.setProperty("password", "mpassword")
      mprop.setProperty("driver", "com.mysql.jdbc.Driver")

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val df = x.map(x => x.split(",")).map(x => (x(0), x(1), x(2))).toDF("name", "age", "city")
      df.createOrReplaceTempView("tab")
      val mas = spark.sql("select * from tab where city='NY'")
      val del = spark.sql("select * from tab where city='CA'")
      val other = spark.sql("select * from tab where city='other'")
      mas.write.mode(SaveMode.Append).jdbc(murl, "newYork", mprop)
      del.write.mode(SaveMode.Append).jdbc(murl, "california", mprop)
      other.write.mode(SaveMode.Append).jdbc(murl, "otherdata", mprop)
      df.show()


    }
    ssc.start()
    ssc.awaitTermination()
  }
}