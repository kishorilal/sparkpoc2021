package projectbasedlearning

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Complaint {
  def main(args: Array[String]):Unit= {
    val spark = SparkSession.builder.master("local[*]").appName("Complaint").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
//    val rdd = sc.textFile("E:\\projectbasedlearning\\complaints.csv");
//    val rdd20 = rdd.take(20)
      val df = spark.read
        .option("header","true")
        .option("inferSchema","true")
        .option("mode","failfast")
        .load("E:\\projectbasedlearning\\data")

df.show(5)
    spark.stop()
  }
}