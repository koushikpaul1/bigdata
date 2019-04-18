package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.functions.{ min, max, avg }
import org.apache.spark.sql.functions.{expr, col, column}
import org.apache.spark.sql._
 import org.apache.spark.sql.types._


object B_dfJsonOperations {
  
  def main (args  : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("B_dfJsonOperations")
    val spark = SparkSession.builder().appName("B_dfJsonOperations").master("local[2]").getOrCreate()
    import spark.implicits._
    val dfJson = spark.read.json("input/Spark-The-Definitive-Guide/flight-data/json/2015-summary.json")
    val dfEmp = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/employees?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "employees").option("user", "root").option("password", "root").load()

   
   //adding a column
  import org.apache.spark.sql.functions.lit
  dfJson.select(expr("*"), lit("Koushik")as ("edge")).show(5) 
  dfJson.withColumn("Koushik", lit("Koushik")).show(5) 
  //dropping a column 
  dfJson.withColumn("isSameCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(5) 
  //Renaming a column
  dfJson.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(5)
  dfEmp.select(expr("emp_no as WTF"))show(5)
  
  //Filter

  }
  
}