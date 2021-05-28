package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrameExample4 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

   val dataLocation = "C:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read
      .format("csv")
      .option("header","true")
      .option("InferSchema","true")
      .option("mode","failfast")
      .load(dataLocation)
    //getting the all the columns names.
//    val allColumnName = df.columns.map(x => col(x))
//    allColumnName.foreach(println)
    //selecting the single column names
    val df1 = df.select($"last_name").where($"last_name".like("%tt%"))
    //selecting the all the column names
    val df2 = df.select("*")
    val df3 = df.select(col("first_name").alias("fname"),col("last_name").alias("lname"),concat_ws(" ",col("first_name"),col("last_name")).alias("full name"))
    //selecting the column name using slice method
    val df4 = df.select(df.columns.slice(0,3).map(x => col(x)):_*)
    df4.show(5,false)

    spark.stop()
  }
}