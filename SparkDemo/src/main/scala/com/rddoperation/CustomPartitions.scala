package com.rddoperation

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.Partitioner
object CustomPartitions {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("CustomPartitions").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val rdd = sc.parallelize(1 to 20)
    val kv = rdd.keyBy(f =>f)
    var i =0

    val rdd1 = kv.partitionBy(new CustomPartitions())

    rdd1.glom().foreach(f=>{
      println("partition"+i)
      i+=1
      f.foreach(println)})
    spark.stop()
  }

}

class CustomPartitions extends  Partitioner{
  override def numPartitions: Int = 3

  override def getPartition(key: Any): Int = {
    val num: Int = toInt(key)
    if (num % 2 == 0) 0
    else if(num%3 ==0) 1
    else new java.util.Random().nextInt(2)+1
  }
    def toInt(x: Any): Int = x match {
      case i: Int => i
    }
}