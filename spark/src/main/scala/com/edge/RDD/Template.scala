package com.edge.RDD

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object Template {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
     val spark=SparkSession.
    builder.
    appName("Template").
    master("local").
    config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    getOrCreate()
    println("Template")
    
    
    
    val sc =new SparkContext("local[4]","Template")
  }

}