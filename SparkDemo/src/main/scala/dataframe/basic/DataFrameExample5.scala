package dataframe.basic


import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object DataFrameExample5 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data2 = Seq(Row(Row("James","","Smith"),"OH","M"),
      Row(Row("Anna","Rose",""),"NY","F"),
      Row(Row("Julia","","Williams"),"OH","F"),
      Row(Row("Maria","Anne","Jones"),"NY","M"),
      Row(Row("Jen","Mary","Brown"),"NY","M"),
      Row(Row("Mike","Mary","Williams"),"OH","M")
    )
    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("state",StringType)
      .add("gender",StringType)
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data2),schema)
    df.select("name.*").show()
   // df.show(4)
    spark.stop()
  }
}