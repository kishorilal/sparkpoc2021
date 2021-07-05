package com

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JDBCOptimization {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JDBCOptimization").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val url = ""
    val prop = new java.util.Properties()
    prop.put("driver","")
    prop.put("user","musername")
    prop.put("password","mpassword")
    prop.put("fetchsize","1000")
    prop.put("batchsize","10000")
    prop.put("numPartitions","1000")
    prop.put("partitionColumn","pincode")
    prop.put("lowerBound","1")
    prop.put("lowerBound","10000000")
    prop.put("sessionInitStatement","delete from ind where livestatus='dead'")
    prop.put("truncate","true")

    val df = spark.read.jdbc(url,"emp",prop).withColumn("rownum", monotonically_increasing_id())
    df.write.mode(SaveMode.Overwrite).jdbc(url,"test",prop)  // delete old test table and recreate.
    //instead of that truncate table (not deleted)... ur using same schema.
    //name string, age int , gen char............ name string, age double, gen string


    /*
    select * from tab where incode between 1 and 1lak... 4k records ... 25 roun trips ...
    select * from tab where incode between 1lak and 2lak... 25 rounds
        select * from tab where incode between 2 and 3lak
            select * from tab where incode between 3 and 4lak
            .....
                select * from tab where incode between 99l and 100lak
     */


    spark.stop()
  }
}