package sqlpractices

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, rank}

import java.util.Properties

object EmployeeSalaryData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EmployeeSalaryData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val employeeDF = spark.read
      .format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("mode","failFast")
      .load("C:\\bigdata\\datasets\\10000Records.csv")
    //spark.sql("use database") //jdbc:mysql://localhost:3306
    //spark.sql("use world")
    val cls = employeeDF.columns.map(x => x.replace(" ",""))
    employeeDF.printSchema()
    val employ = employeeDF.toDF(cls:_*)
    employ.printSchema()
    val subSetEmployee = employ.select("empid","salary")
    subSetEmployee.createOrReplaceTempView("emdata")
    spark.sql(
      """
        |select salary from emdata t where 5=(select count(salary) from emdata t1 where t1.salary=t.salary)
        |""".stripMargin).show(5)
    val url = "jdbc:mysql://localhost:3306/world?useSSL=false"
    //setting the property
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","root")  //error Access denied for user 'admin123'@'171.76.106.215' (using password: YES) either user name or password wrong
    prop.put("driver","com.mysql.jdbc.Driver")
   // subSetEmployee.write.mode(SaveMode.Append).jdbc(url,"emp1",prop)

    spark.close()


  }
}
