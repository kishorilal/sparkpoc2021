package com.training.data

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JsonData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("JsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read
      .format("json")
      .option("mode","failfast")
      .load("C:\\bigdata\\datasets\\companies.json")
   // df.createOrReplaceTempView("tab")
    //spark.sql("""select * from tab""").show(false)
    //df.show(false)
    //val df1 = df.withColumn("category",$"categories"(0))
    df.printSchema();
    df.show(20,false)
    spark.stop()//
  }
}
/*
* val rs=spark.sql("select _id.`$oid` oid ," +
      "pr.is_past,pr.provider.name provider_name,pr.provider.permalink provider_permalink,pr.title as provider_title," +
      "rel.is_past,rel.title person_title,rel.person.first_name person_first_name,rel.person.last_name person_last_name," +
      "rel.person.permalink person_permalink," +
      "tag_list,total_money_raised,twitter_username,updated_at,alias_list," +
      "blog_feed_url,blog_url,category_code,created_at,crunchbase_url,vem.description as video_embeds_descrp," +
      "vem.embed_code,scs.attribution,scs.available_sizes[1] as available_size1," +
      "scs.available_sizes[2] as available_size2,scs.available_sizes[3] as available_size3," +
      "ipo.pub_day,ipo.pub_month,ipo.pub_year,ipo.stock_symbol,ipo.valuation_amount,ipo.valuation_currency_code ," +
      "acquisition.acquired_day,acquisition.acquired_month,acquisition.acquired_year," +
      "aq.acquired_day,aq.acquired_month,aq.acquired_year,aq.company.name company_name,aq.company.permalink," +
      "acquisition.acquiring_company.name aquire_company_name,acquisition.acquiring_company.permalink," +
      "cmp.competitor.name as competitor_name,cmp.competitor.permalink as competition_permalink," +
       "deadpooled_day,deadpooled_month,deadpooled_url,deadpooled_year,description,email_address," +
      "video_embeds[0] video_embeds1,video_embeds[1] video_embeds2,video_embeds[2] video_embeds3" +
      " from  tab lateral view explode(acquisitions) tmp as aq lateral view explode(competitions) tmp as cmp " +
      "lateral view explode(video_embeds) tmp as vem lateral view explode(screenshots) tmp as scs " +
      "lateral view explode(relationships) tmp rel lateral view explode(providerships) tmp pr ")

* */