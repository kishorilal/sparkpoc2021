package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DistinctElement {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DistinctElement").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = Seq(("Banana",1000,"OH","USA"), ("Carrots",1500,"OH","USA"), ("Beans",1600,"OH","USA"),
      ("Orange",2000,"OH","USA"),("Orange",2000,"OH","USA"),("Banana",400,"OH","China"),
      ("Carrots",1200,"OH","China"),("Beans",1500,"OH","China"),("Orange",4000,"OH","China"),
      ("Banana",2000,"OH","Canada"),("Carrots",2000,"OH","Canada"),("Beans",2000,"OH","Mexico"))

    val df = data.toDF("Product","Amount","State","Country")
    df.show()
    println("============================ Pivot method ================================")
    val df_Pivot = df.groupBy("Product").pivot("Country").sum("Amount")
    df_Pivot.show(false)
    val pivotDF = df.groupBy("Product","Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    pivotDF.show()
    println("============================= UnPivot method =============================")
    val unPivotDF = pivotDF.select($"Product",
      expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
      .where("Total is not null")
    unPivotDF.show()
  }
}