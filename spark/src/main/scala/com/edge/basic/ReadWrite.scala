package com.edge.basic

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object ReadWrite {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    sparkContext
    sparkSession

  }
  def sparkContext() {
    def sc = new SparkContext("local[*]", "ReadWrite")
    val lineRDD = sc.textFile("input/udemy/spark-scala/ml-100k/u.data", 5)

  }
  def sparkSession() {
    val spark = SparkSession.
      builder.
      appName("ReadWrite").
      master("local[*]").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
    println("sparkSession")

    val csvDF = spark.read.csv("data.csv")
    val jsonDF = spark.read.json("data.json")
    val textDF = spark.read.text("data.txt")
    val parquetDF = spark.read.parquet("data.txt")

      val parsedCSVDF = spark.read.
      option("header", "true").
      option("nullValue", "?").
      option("inferSchema", "true").
      csv("input/Advanced-Analytics-with-Spark/linkage")
    
    
    csvDF.createTempView("csvTable")
    jsonDF.createTempView("jsonTable")
    textDF.createTempView("textTable")

    import spark.implicits._
    spark.sqlContext.sql("select * from someTable")

  }

}