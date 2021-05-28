package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StructField, StructType,StringType,IntegerType}

object DataFrameExample2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header","true")
      .option("InferSchema","true")
      .option("mode","failfast")
      .load("C:\\bigdata\\datasets\\user_book.csv")
    val rdd  = sc.textFile("C:\\bigdata\\datasets\\user_book.csv",2)
    val schema = StructType(Array(StructField("Book_name",StringType,true),StructField("Price",StringType,false)))
    val skip = rdd.first()
    val rdd1 = rdd.filter(f => f!=skip).map(x => x.split(",")).map(x => (x(0),x(1).toString))
    val rdd2 =  rdd1.map(x => Row(x._1,x._2))
    val df1 = spark.createDataFrame(rdd2,schema)
    //rdd1.collect().foreach(println)
    df1.show()
    spark.stop()
  }
}