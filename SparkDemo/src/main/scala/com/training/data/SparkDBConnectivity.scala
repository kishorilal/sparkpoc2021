package com.training.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
  import java.util.Properties

  object SparkDBConnectivity {
    def main(args: Array[String]) {
      val spark = SparkSession.builder.master("local[*]").appName("mssqlOracle").getOrCreate()
      //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
      val sc = spark.sparkContext
      sc.setLogLevel("ERROR")
      import spark.implicits._
      import spark.sql
      //connect mssql
      val msurl = "jdbc:sqlserver://perfdb.cqwod5o34rlo.eu-central-1.rds.amazonaws.com:1433;databaseName=testingdb"

      val msprop = new Properties()
      msprop.put("user","msusername")
      msprop.put("password","mspassword")
      msprop.put("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
      //val df = spark.read.jdbc(msurl,"EMP",msprop)
      val qry = "(select e.sal from emp e join dept d on e.deptno = d.deptno where e.sal>1500) abcd"
      val df = spark.read.jdbc(msurl,qry,msprop)
//      val tabs = Array("DEPT", "EMP")
//      tabs.foreach { x=>
//        val df = spark.read.jdbc(msurl,s"$x",msprop)
//        val ourl = "jdbc:oracle:thin:@//ora.cqwod5o34rlo.eu-central-1.rds.amazonaws.com:1521/ORCL"
//        val oprop = new Properties()
//        oprop.put("user","ousername")
//        oprop.put("password","opassword")
//        oprop.put("driver","oracle.jdbc.OracleDriver")
//        df.write.jdbc(ourl,s"$x",oprop)
        df.show()
    //  }

      spark.stop()
    }
  }
