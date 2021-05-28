package Spark.definitive.book

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object FirstExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("FirstExample").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val rdd = sc.textFile("E:\\spark_definitive\\spark\\data\\flight-data\\csv\\2015-summary.csv")
    val skip = rdd.first()
    val rdd1 = rdd.filter(f => f!=skip).map(x =>x.split(",")).map(x =>(x(0),x(1),x(2)))
    val rdd2 = rdd1.filter(x=>x._1.trim.equals("United States") && x._2.trim.equals("India"))

    rdd2.take(5).foreach(println)
    spark.stop()
  }
}