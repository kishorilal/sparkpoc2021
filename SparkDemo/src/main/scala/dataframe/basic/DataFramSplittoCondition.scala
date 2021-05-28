package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType, StructField, StructType}

object DataFramSplittoCondition {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    //will discuss the two conditional statement
    //where condition and filter
    val data = "C:\\bigdata\\datasets\\us-500.csv"
    val schema = new StructType(Array(
      StructField("first_name",StringType,false),StructField("last_name",StringType,true),
      StructField("company_name",StringType,false),StructField("address",StringType,true),
      StructField("city",StringType,false),StructField("county",StringType,true),
      StructField("state",StringType,false),StructField("zip",IntegerType,true),
      StructField("phone1",StringType,false),StructField("phone2",StringType,true),
      StructField("email",StringType,false),StructField("web",StringType,true),
      StructField("_corrupt_record",StringType,false)
    ))
    //first_name,last_name,company_name,address,city,county,state,zip,phone1,phone2,email,web
    val t1 = System.currentTimeMillis()
    val df = spark.read
      .format("csv")
      .option("mode","permissive")
      .option("header","true")
      .schema(schema)
      .load(data)
    //val df1 = df.groupBy("state").count()
   df.show(false)

    spark.stop()
  }
}