package com.neo

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{to_timestamp, _}

object NeoDataAnalysis2 {
  def main(args: Array[String]) :Unit= {
    val spark = SparkSession.builder.master("local[*]").appName("NeoDataAnalysis").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data = "E:\\spark_definitive\\logdata\\Data\\CFC1000.txt"
    val rdd = sc.textFile(data,1)

    val firstSkip = rdd.first()
    val filterRdd = rdd.filter(x => (x!=firstSkip) && (x.contains("Write operation SetAttribute: Arguments") && x.contains("CFC") || x.contains("Created successfully")))
    filterRdd.cache()
    val processStartId = filterRdd.filter(_.contains("CFC_999")).map(x =>x.split('|')).map(x=>x(0).trim.toLong).take(1)(0)
    var i = 0
    val creationStartTimeRDD = filterRdd.filter(x =>x.contains("Write operation SetAttribute: Arguments") && x.contains("CFC"))
    val creationStartTimeSplit = creationStartTimeRDD.map(x =>x.split('|')).filter(x =>x(0).trim.toLong>=processStartId).map(x =>{
      i =i+1
      (x(0).trim.toLong,x(8).trim,x(10).trim,i)
    })
//    val startCreationID = creationStartTimeSplit.zipWithIndex().filter(x => x._1._3.contains("CFC_999"))
//    val processStartId = startCreationID.map(x =>x._1._1).take(1)(0)
//    val objectCreationTime = creationStartTimeSplit.filter(x =>x._1>=processStartId)
    val startTimeDF = creationStartTimeSplit.toDF("rowId","startTime","objectName","id")
    val replaceDot = udf(replaceString _)
    val startTimeDF1 = startTimeDF.withColumn("startTime",to_timestamp(replaceDot($"startTime")))
    val startTimeDF2 = startTimeDF1.sort("startTime")

    startTimeDF2.coalesce(1).write.format("csv").mode(saveMode = "append").save("E:\\save\\dataNeo\\start")
    var j = 0
    val creationEndTimeRDD = filterRdd.filter(x =>x.contains("Created successfully"))
    val creationEndTimeSplit = creationEndTimeRDD.map(x =>x.split('|')).filter(x =>x(0).trim.toLong>=processStartId).map(x =>{
      j = j + 1
      (x(0).trim.toLong,x(8).trim,x(10).trim,j)
    })
    val endTimeDF = creationEndTimeSplit.toDF("rowId","endTime","success","id")
    val endTimeDF1 = endTimeDF.withColumn("endTime",to_timestamp(replaceDot($"endTime")))
    val endTimeDF2 = endTimeDF1.sort("endTime")
    endTimeDF2.coalesce(1).write.format("csv").mode(saveMode = "append").save("E:\\save\\dataNeo\\end")
//
   val join = endTimeDF2("id") === startTimeDF2("id")
   val joinStartEndTime = startTimeDF2.join(endTimeDF2,join,"inner")

    //val timeDifference = joinStartEndTime.select(col("objectName"),col("endTime")-col("startTime"))//joinStartEndTime.select(col("objectName"),(col("endTime").cast("long")-col("startTime").cast("long")).alias("creationTime"))
    //timeDifference.coalesce(1).write.format("csv").mode(saveMode = "append").save("E:\\save\\dataNeo\\end")
    spark.stop()
  }
  def replaceString(value:String):String={
    var arr = value.split(" ")
    arr(0) = arr(0).replace(".","-")
    arr(0)+" "+arr(1)
  }
}