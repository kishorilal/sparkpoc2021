package com.spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object kafkaConsumerNifi {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.streaming.kafka.allowNonConsecutiveOffsets","true").appName("kafkaConsumer").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    //val sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "aaaaa",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("abcd")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // dstream created

    val lines=  stream.map(record =>  record.value)
    lines.print()
    lines.foreachRDD { x =>
      val spark = SparkSession.builder.config(x.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      //val reg = "(?P<ip>.?) (?P<remote_log_name>.?) (?P<userid>.?) \[(?P<date>.?)(?= ) (?P<timezone>.?)\] \"(?P<request_method>.?) (?P<path>.?)(?P<request_version> HTTP/.)?\" (?P<status>.?) (?P<length>.?) \"(?P<referrer>.?)\" \"(?P<user_agent>.?)\" (?P<session_id>.?) (?P<generation_time_micro>.?) (?P<virtual_host>.*)"

val df = spark.read.json(x)
df.show()
      /*val murl="jdbc:mysql://mysqldb.ckrdcsh7dnpl.ca-central-1.rds.amazonaws.com:3306/mysqldb"
      import java.util.Properties
      val mprop = new Properties()
      mprop.put("user","myusername")
      mprop.put("password","mypassword")
      mprop.put("driver","com.mysql.cj.jdbc.Driver")
      //spark.sql("show tables;")
      df.write.mode(SaveMode.Append).jdbc(murl,"kafkadata30june",mprop)
*/

      /* df.createOrReplaceTempView("tab")
      val res = spark.sql("select * from tab where city='mas'")
      val res1 = spark.sql("select * from tab where city='del'")
      res.write.mode(SaveMode.Append).jdbc(ourl,"masinfo",oprop)
      res1.write.mode(SaveMode.Append).jdbc(ourl,"delhiinfo",oprop)
*/
    }
    ssc.start()             // Start the computation
    ssc.awaitTermination()

  }
}