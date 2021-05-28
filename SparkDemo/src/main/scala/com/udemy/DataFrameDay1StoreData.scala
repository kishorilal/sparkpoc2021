package com.udemy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrameDay1StoreData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameDay1").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read
      .format("json")
      .option("inferSchema","true")
      .option("header","true")
      .option("mode","failfast")//1.failfast 2.permissive 3.dropmaleformed
      .load("E:\\Spark_Udemy\\store_locations.json")
   // val df3 = df.repartition(col("city"))
    val df1 = df.groupBy($"city").count().alias("count")//df.randomSplit(Array(.7,.3,30))//df.sample(true,.5,30)//select($"state").distinct()//df.where($"city".like("%o%"))//df.groupBy("city").count()
 // val df2 = df.where(($"city" ==="Chico" && $"state" === "CA") && ($"zip_code" ===959284422))
    //val df2 = df.select($"city").groupBy("city").count().alias("count").orderBy($"city".desc)//df1.orderBy(col("count").desc)
    //df2.write.format("json").mode("append").save("E:\\save\\stor.json")
    //val df2 = df1.orderBy(col("count").desc)
    val df2 = df1.orderBy(desc("count"),asc("city"))

   df2.show(20)

    spark.stop()
  }
}