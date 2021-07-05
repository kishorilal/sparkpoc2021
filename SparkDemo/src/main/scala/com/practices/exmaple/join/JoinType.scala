package com.practices.exmaple.join

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JoinType {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DistinctElement").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
      (2,"Rose",1,"2010","20","M",4000),
      (3,"Williams",1,"2010","10","M",1000),
      (4,"Jones",2,"2005","10","F",2000),
      (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
    )
    val empColumns = Seq("emp_id","name","superior_emp_id","year_joined",
      "emp_dept_id","gender","salary")
    val empDF = emp.toDF(empColumns:_*)
    empDF.show(false)

    val dept = Seq(("Finance",10),
      ("Marketing",20),
      ("Sales",30),
      ("IT",40)
    )

    val deptColumns = Seq("dept_name","dept_id")
    val deptDF = dept.toDF(deptColumns:_*)
    deptDF.show(false)

    println("========================== join type inner ======================================")
    val col = empDF("emp_dept_id") === deptDF("dept_id")
    empDF.join(deptDF,col,"inner").show(false)
    println("========================== outer join ======================================")

    empDF.join(deptDF,col,"outer").show(false)
    println("========================== full join ======================================")

    empDF.join(deptDF,col,"full").show(false)
    println("========================== fullouter join ======================================")

    empDF.join(deptDF,col,"fullouter").show(false)
    println("========================== full_outer join======================================")
    empDF.join(deptDF,col,"full_outer").show(false)
    println("========================== left join======================================")
    empDF.join(deptDF,col,"left").show(false)
    println("========================== left outer======================================")
    empDF.join(deptDF,col,"left_outer").show(false)
    println("========================== leftouter join======================================")
    empDF.join(deptDF,col,"leftouter").show(false)
    println("========================== right outer join======================================")
    empDF.join(deptDF,col,"right").show(false)
    println("========================== right outer join======================================")
    empDF.join(deptDF,col,"rightouter").show(false)
    println("========================== right_outer join======================================")
    empDF.join(deptDF,col,"right_outer").show(false)
    println("========================== semi join======================================")
    empDF.join(deptDF,col,"leftsemi").show(false)
    println("========================== anti join======================================")
    empDF.join(deptDF,col,"leftanti").show(false)
    println("========================== crossjoin======================================")
    empDF.join(deptDF,col,"cross").show(false)


    spark.stop()
  }
}