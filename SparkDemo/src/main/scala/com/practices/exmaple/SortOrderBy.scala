package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SortOrderBy {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DistinctElement").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("James","Sales","NY",90000,14,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.show()
    df.cache()
    println("====================================== sort the by employee_name ==========================================")
    df.sort(col("employee_name")).show(3,false)
    println("====================================== sort the by employee_name  and age ==========================================")
    df.sort(col("employee_name"),col("age")).show(3,false)
    println("====================================== sort the by employee_name ==========================================")
    df.orderBy(col("employee_name")).show(3,false)
    println("====================================== sort the by employee_name  and age ==========================================")
    df.orderBy(col("employee_name"),col("department")).show(3,false)
    println("======================================  DSL ==========================================")
    df.select($"employee_name",asc("department"),desc("state")).show(3,false)
    println("====================================== SQL ==========================================")
    df.createOrReplaceTempView("tab")
    spark.sql("select employee_name,asc(department),desc(start) from tab").show(false)
    spark.stop()
  }
}