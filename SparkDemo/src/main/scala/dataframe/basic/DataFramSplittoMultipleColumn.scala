package dataframe.basic

import org.apache.spark.sql._

object DataFramSplittoMultipleColumn {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
      ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    val df = spark.createDataFrame(data).toDF(columns:_*)
    val df1 = df.map(f =>{
      val name = f.getString(0).split(",")
      val address = f.getString(1).split(",")
      (name(0),name(1),address(0),address(1),address(2))
    })
    df1.show(false)
    spark.stop()
  }
}