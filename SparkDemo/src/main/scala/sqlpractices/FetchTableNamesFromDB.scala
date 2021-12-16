package sqlpractices

import org.apache.spark.sql.{DataFrame, SparkSession}



object FetchTableNamesFromDB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("FetchTableFromDB")
      .getOrCreate()

    val tbl = getDBInformation(spark,"tables")
      .where("table_schema='world'")
      .select("table_name")
      .collect()
    tbl.foreach(println)
    spark.stop();

  }
  def getDBInformation(spark:SparkSession,typeInfo:String) :DataFrame={
    val username="root"
    val password="root"
    val driver="com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/world?user=root&password=root&allowPublicKeyRetrieval=true&useSSL=false"
    val df = spark.read
      .format("jdbc")
      .option("username",username)
      .option("password",password)
      .option("driver",driver)
      .option("dbtable","information_schema."+typeInfo)
      .option("url",url)
      .load()
    df
  }
}
