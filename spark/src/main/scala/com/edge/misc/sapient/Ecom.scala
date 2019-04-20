package com.edge.misc.sapient


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object Ecom extends App {
  println("Sapient")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.
    builder.
    appName("Ecom").
    master("local").
    config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    getOrCreate()
        
    val dfEcomCompetitor = spark.read.format("csv").option("delimiter", "|").option("inferSchema", "true").option("header", "true").load("input/sapient/ecom/ecom_competitor_data.txt")
     val dfProduct = spark.read.format("csv").option("delimiter", "|").option("inferSchema", "true").option("header", "true").load("input/sapient/ecom/internal_product_data.txt")
      val dfSeller = spark.read.format("csv").option("delimiter", "|").option("inferSchema", "true").option("header", "true").load("input/sapient/ecom/seller_data.txt")
 
      dfEcomCompetitor.createOrReplaceTempView("ecom_competitor")
    dfProduct.createOrReplaceTempView("internal_product")
    dfSeller.createOrReplaceTempView("seller")
    
    
    
    
    
}