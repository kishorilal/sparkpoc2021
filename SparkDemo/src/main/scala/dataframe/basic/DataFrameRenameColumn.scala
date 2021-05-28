package dataframe.basic


import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object DataFrameRenameColumn {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("DataFrameExample").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    val data = Seq(Row(Row("James ","","Smith"),"36636","M",3000),
      Row(Row("Michael ","Rose",""),"40288","M",4000),
      Row(Row("Robert ","","Williams"),"42114","M",4000),
      Row(Row("Maria ","Anne","Jones"),"39192","F",4000),
      Row(Row("Jen","Mary","Brown"),"","F",-1)
    )
    val schema = new StructType()
      .add("full name",new StructType()
        .add("f_name",StringType)
        .add("m_name",StringType)
        .add("l_name",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("Salary",IntegerType)

   val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  //renaming the nested column
    val df1 = df.select(col("full name.f_name").as("first_name"))
    //using struct type
    val schem1 = new StructType()
      .add("full name.f_name",StringType)
      .add("full name.m_name",StringType)
      .add("full name.l_name",StringType)
    val df2 = df.select(col("full name").cast(schem1),$"dob",$"gender",$"salary")
//using multiple column and
    val old_column =Seq("full name","dob","gender","salary")
    val new_column =Seq("full_name1","dob1","gender1","salary1")
    val newNames = old_column.zip(new_column).map(f =>{col(f._1).as(f._2)})
    val df3 = df.select(newNames:_*)
    df3.show(false)
    df3.printSchema()
    df2.show(false)
//    val df1 = df.withColumnRenamed("dob","date_of_birth").withColumnRenamed("salary","salary_amount")
//    val df2 = df1.withColumn("date_of_birth",col("date_of_birth").cast(IntegerType))
//    val df3 = df2.withColumn("date_of_birth",col("date_of_birth").cast("String"))
//      df3.createOrReplaceTempView("emp")
//
//    val df4 = spark.sql("select * from emp where date_of_birth='36636'")
//    df4.show(false)
//    df3.printSchema()
    spark.stop()
  }
}