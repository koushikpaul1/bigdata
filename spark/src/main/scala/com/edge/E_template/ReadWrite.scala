package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
 import scala.io.Source
class ReadWrite {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.
      builder.
      appName("ReadWrite").
      master("local[*]").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
      
       val lines = Source.fromFile("input/udemy/spark-scala/ml-100k/u.item").getLines()
      
      
      
  }
  
}