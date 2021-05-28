package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameExample3 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    //creating the data frame from list
    val list = List(12,3,4,5,66)
    val df = list.toDF("count")
    val schema = StructType(Array(StructField("count1",IntegerType),StructField("count2",IntegerType)))
    //creating the data frame from seq
    val se = Seq(Row(1,2),Row(3,4),Row(5,6))
    //val df1 = se.toDF("first","second pair")
    import scala.collection.JavaConversions._
    val df1 = spark.createDataFrame(se,schema)
    df1.show()
    spark.stop()
  }
}