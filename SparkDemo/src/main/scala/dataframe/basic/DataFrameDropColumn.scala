package dataframe.basic

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DataFrameDropColumn {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )
    val schema = new StructType()
      .add("full name",new StructType()
        .add("f_name",StringType)
        .add("m_name",StringType)
        .add("l_name",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("Salary",IntegerType)

   val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
 val df1 = df.select(col("full name.f_name").as("first_name"),col("full name.m_name").as("middle_name")
 ,col("full name.l_name").as("last_name"),col("dob"),$"gender",col("salary"))
    val df2 = df1.drop("first_name","dob").where($"dob"==="40288")

    df2.show(false)
    spark.stop()
  }
}