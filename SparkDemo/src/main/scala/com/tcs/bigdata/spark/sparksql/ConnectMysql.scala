package com.tcs.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object ConnectMysql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ConnectMysql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val url = "jdbc:mysql://mysql.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:3306/employee"
   //setting the property
    val prop = new Properties()
    prop.put("user","admin")
    prop.put("password","admin123")  //error Access denied for user 'admin123'@'171.76.106.215' (using password: YES) either user name or password wrong
    prop.put("driver","com.mysql.jdbc.Driver")
    //getting the table value
    println("=========================simple conection==========================")
    val df = spark.read.jdbc(url,"emp",prop)
    df.show(5,false)
    println("=========================specifiying the query in conection==========================")
    val query = "(select * from EMP where sal>1500) t"
    val df_query = spark.read.jdbc(url,"emp",prop)
    df_query.show(5,false)
    println("=========================specifiying the join query ==========================")
    val query_join = "(select e.empno,e.ename,e.job from emp e JOIN dept d On e.deptno = d.deptno where sal>1500) t"
    val df_join = spark.read.jdbc(url,query_join,prop)
    df_join.show(5,false)
    println("=========================getting the data from multiple table ==========================")
    val tab  = Array("emp","dept")
    for(i<- tab){
      val df_table = spark.read.jdbc(url,s"$i",prop)
      df_table.show(5,false)
    }
    println("=========================getting data from multiple table and storing to data base ==========================")
/*
    val oProp =new Properties()
    val ourl = "jdbc:oracle:thin:@//employee.ce5lmvnp4idz.ap-south-1.rds.amazonaws.com:1521/ORCL"
    oProp.put("user","admin")
    oProp.put("password","admin123")
    oProp.put("driver","oracle.jdbc.OracleDriver")
    for(i <- tab){
      val df_table = spark.read.jdbc(url,s"$i",prop)
      df_table.write.mode("overwrite")jdbc(ourl,s"$i",oProp)

    }//default save mode errorifxists
    println("=========================get the data from multiple table in oracle database ==========================")

    for(i <- tab){
      val df_table = spark.read.jdbc(ourl,s"$i",oProp)
      df.show(5,false)

    }*/
    spark.stop()
  }
}