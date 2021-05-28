package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat_ws, lit}
import org.apache.spark.sql.types.{StringType, StructType}

object DataFrameExample6 {
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
    val t1 = System.currentTimeMillis()
   // val dfs = df.select($"first_name",$"last_name".alias("lname"),lit("USA")).where($"lname"==="Foller")
  //  val df1 = df.withColumn("first_name",$"first_name").withColumn("lname",$"last_name").withColumn("country",lit("USA")).where($"lname"==="Foller")
   val dfs = df.drop($"first_name")
    val dfs1 = dfs.drop($"first_name")
    dfs1.show(5,false)
   // df1.show(5,false)
    println((System.currentTimeMillis()-t1))

    spark.stop()
  }
}