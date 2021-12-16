package sqlpractices

import org.apache.spark.sql.SparkSession

object EmployeeSalaryData1 {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("EmployeeSalaryData")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val employeeDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("mode", "failFast")
      .load("C:\\bigdata\\datasets\\10000Records.csv");
    val columnNames = employeeDF.columns.map(x => x.replace(" ",""))

    val employeeData = employeeDF.toDF(columnNames:_*)
    employeeData.show(5)
  }
}
