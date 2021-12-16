package olc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RDDTranformation {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("OLC").master("local").getOrCreate();

    val sc = sparkSession.sparkContext;
    sc.setLogLevel("Error")
    val sparkConf = new SparkConf().setAppName("Demo").setMaster("local")
    sparkConf.set("spark.driver.allowMultipleContexts","true")

    val sparkconext = new SparkContext(sparkConf);
   val rdd =  sc.parallelize(1 to 10 toList)
    rdd.intersection(rdd);
    println(sc)
    println(sparkconext)
    sparkSession.close()
  }
}
