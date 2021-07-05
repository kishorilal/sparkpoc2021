package com.practices.exmaple

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DF_Pivot {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DistinctElement").getOrCreate()
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
    println("============================ Distinct method ================================")
    val df_Distinct = df.distinct()
    df_Distinct.show()
    println("============================= Drop Duplicates =================================")
    val df_DropDuplicate = df.dropDuplicates("salary","department")
    df_DropDuplicate.show(false)
    spark.stop()
    val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico"))

  }
}