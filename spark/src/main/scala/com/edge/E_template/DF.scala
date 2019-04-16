package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.functions.{ min, max, avg }

object DF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.
    builder().
    appName("DF").
    master("local[2]").
    config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    getOrCreate()

  import spark.implicits._
  import spark.sql

  val dfMyRange = spark.range(10010).toDF("number")
  val divisBy2 = dfMyRange.where("number % 2 = 0")
  println(divisBy2.count()) //500
  dfMyRange.createOrReplaceTempView("dfTableRange")

  val dfJson = spark.read.json("input/Spark-The-Definitive-Guide/flight-data/json/2015-summary.json")
  val jsonSchema = dfJson.schema
  println(jsonSchema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,LongType,true))
  dfJson.createOrReplaceTempView("dfTableJson")

  val dfCSV = spark
    .read.format("csv")
    .load("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfCSV.schema) //StructType(StructField(_c0,StringType,true), StructField(_c1,StringType,true), StructField(_c2,StringType,true))
  dfCSV.createOrReplaceTempView("dfTableCSV")

  val dfCSVInferSchema = spark
    .read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfCSVInferSchema.schema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,IntegerType,true))
  dfCSVInferSchema.createOrReplaceTempView("dfTableCSVInfer")

  import org.apache.spark.sql.types._

  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)))
  val dfJsonWithSchema = spark.read.schema(myManualSchema).csv("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfJsonWithSchema.schema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,LongType,true))
  dfJsonWithSchema.createOrReplaceTempView("dfTableCSVSchema")

  import org.apache.spark.sql._
  val dfMysqlemp = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/employees?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "employees").
    option("user", "root").
    option("password", "root").
    load()
  println(dfMysqlemp.schema) //StructType(StructField(emp_no,IntegerType,true), StructField(birth_date,DateType,true), StructField(first_name,StringType,true), StructField(last_name,StringType,true), StructField(gender,StringType,true), StructField(hire_date,DateType,true))
  //dataframe_employees.show(10)
  dfMysqlemp.createOrReplaceTempView("dfTableEmp")

  sql("select * from dfTableEmp e join dfTableRange r on e.emp_no=r.number").show

}
