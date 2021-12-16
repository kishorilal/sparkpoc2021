package Spark.definitive.book

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object KafkaStreamDemo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("KafkaStreamDemo")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.sql.streaming.schemaInference","true")
      .getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      ))))
    ))

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","invoices")
      .option("startingOffsets","earliest")
      .load()
    //    val valueDF = kafkaDF.select(from_json(col("value").cast("string"),schema).as("value"))
    //    val explodeDF = valueDF.selectExpr("CESS","CGST","CashierID","CreatedTime","CustomerCardNo","CustomerType",
    //      "DeliveryType","CustomerCardNo","DeliveryType","InvoiceNumber","NumberOfItems","PaymentMethod","PosID","SGST",
    //      "StoreID","TaxableAmount","TotalAmount","DeliveryAddress.AddressLine","DeliveryAddress.City","DeliveryAddress.ContactNumber","DeliveryAddress.PinCode","DeliveryAddress.State",
    //      "explode(InvoiceLineItems) as LineItem")
    //    val flattenDF = explodeDF.withColumn("ItemCode",$"LineItem.ItemCode")
    //      .withColumn("ItemCode",$"LineItem.ItemCode")
    //      .withColumn("ItemDescription",$"LineItem.ItemDescription")
    //      .withColumn("ItemPrice",$"LineItem.ItemPrice")
    //      .withColumn("ItemQty",$"LineItem.ItemQty")
    //      .withColumn("TotalValue",$"LineItem.TotalValue")
    //      .drop("LineItem")
    //
    //
    //    val query = flattenDF.writeStream
    //      .format("json")
    //      .option("path","output")
    //      .queryName("jsondata")
    //      .outputMode("complete")
    //      .start()
    //    query.awaitTermination()
    kafkaDF.printSchema()
  }
}