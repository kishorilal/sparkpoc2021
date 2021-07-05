package com.sqlfunction

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AggregateFunction {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("AggregateFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")
    df.groupBy($"department").agg(sum($"salary").as("sum"),
      max($"salary"),
      min(col("salary")),
      avg(col("salary")))
      .orderBy(asc("sum"))
      .where($"sum">6000)
      .show(false)
    df.select(collect_list("salary")).show(false)
    //df.show()
    spark.stop()
  }
}