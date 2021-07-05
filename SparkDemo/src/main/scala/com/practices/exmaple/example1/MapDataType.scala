package com.practices.exmaple.example1

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, MapType, StringType, StructField, StructType}

object MapDataType {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("UDFFunction").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val arrayStructureData = Seq(
      Row("James",List(Row("Newark","NY"),Row("Brooklyn","NY")),
        Map("hair"->"black","eye"->"brown"), Map("height"->"5.9")),
      Row("Michael",List(Row("SanJose","CA"),Row("Sandiago","CA")),
        Map("hair"->"brown","eye"->"black"),Map("height"->"6")),
      Row("Robert",List(Row("LasVegas","NV")),
        Map("hair"->"red","eye"->"gray"),Map("height"->"6.3")),
      Row("Maria",null,Map("hair"->"blond","eye"->"red"),
        Map("height"->"5.6")),
      Row("Jen",List(Row("LAX","CA"),Row("Orange","CA")),
        Map("white"->"black","eye"->"black"),Map("height"->"5.2"))
    )

    val mapType  = DataTypes.createMapType(StringType,StringType)

    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("addresses", ArrayType(new StructType()
        .add("city",StringType)
        .add("state",StringType)))
      .add("properties", mapType)
      .add("secondProp", MapType(StringType,StringType))

    val mapTypeDF = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
    mapTypeDF.printSchema()
   // mapTypeDF.show(false)
    mapTypeDF.select('name,explode('addresses)).show(false)
    mapTypeDF.select('name,map_keys($"properties")).show(false)
    mapTypeDF.select('name,map_values($"properties")).show(false)
       spark.stop()
  }
}