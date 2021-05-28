package dataframe.basic

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataFrameExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val seq = Seq(("Java",500),("C++",250),("spark learning",800),("Java",500))
    val Columns = Seq("Book_name","Price")
    //defining the column name
   // val df = sc.parallelize(seq).toDF("language","Price")
    //without column name
    //val df = sc.parallelize(seq).toDF()
    //creating the df using createdataframe method

    val df = spark.createDataFrame(seq).toDF(Columns:_*)
    df.createOrReplaceTempView("Book")
    val dftmp = spark.sql("select sum(Price) from Book group by Book_name")
    dftmp.show()
    //val df1 = df.agg(sum($"Price")).groupBy($"Book name")
    val dff = df.groupBy($"Book_name").agg(sum($"Price").alias("Sum_of_Book"))
    //df.printSchema()
    //dff.show()
    //rdd.collect().foreach(x=>println(x.getString(0) +"  "+x.getInt(1)))
    spark.stop()
  }
}