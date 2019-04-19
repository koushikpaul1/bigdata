package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.functions.{expr, col, column,min, max, avg,desc,asc}
import org.apache.spark.sql._
 import org.apache.spark.sql.types._


object B_dfJsonOperations {
  
  def main (args  : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("B_dfJsonOperations")
    val spark = SparkSession.builder().appName("B_dfJsonOperations").master("local[2]").getOrCreate()
    import spark.implicits._
    val dfJson = spark.read.json("input/Spark-The-Definitive-Guide/flight-data/json/2015-summary.json")
    val dfCsv = spark.read.option("inferSchema","true").option("header", "true").csv("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
    val dfEmp = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/employees?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "employees").option("user", "root").option("password", "root").load()

    //adding a column
    import org.apache.spark.sql.functions.lit
    dfJson.select(expr("*"), lit("Koushik") as ("edge")).show(5)
    dfJson.withColumn("Koushik", lit("Koushik")).show(5)
    dfJson.withColumn("isSameCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME")).show(5) // populating value
    //dropping a column
    dfJson.drop("ORIGIN_COUNTRY_NAME").columns
    //Renaming a column
    dfJson.withColumnRenamed("DEST_COUNTRY_NAME", "dest").show(5)
    dfEmp.select(expr("emp_no as WTF")) show (5)

    //Filter
    dfJson.filter("count < 2").show(2)
    dfJson.filter(col("count") < 2).show(2)
    dfJson.where("count < 2").show(2)

    dfJson.where("count < 2").filter("ORIGIN_COUNTRY_NAME != 'Croatia'").show(2)
    dfJson.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia").show(2)

    //distinct
    dfJson.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().show(5)

    //union
    println(dfJson.count())
    println(dfJson.union(dfCsv).count())
    dfJson.union(dfCsv).where("count = 1").where($"ORIGIN_COUNTRY_NAME" =!= "United States").show()

    //sort
    import org.apache.spark.sql.functions.{ desc, asc }
    dfJson.sort("count").show(5)
    dfJson.orderBy("count", "DEST_COUNTRY_NAME").show(5)
    dfJson.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)
    dfJson.orderBy(expr("count desc")).show(5)
    dfJson.orderBy(desc("count")).show(5)
    dfJson.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(5)

    //Repartition used for increasing the number of partition
    println(dfJson.rdd.getNumPartitions) // 1
    dfJson.repartition(5)
    dfJson.repartition(col("DEST_COUNTRY_NAME"))
    dfJson.repartition(5, col("DEST_COUNTRY_NAME"))
    println(dfJson.rdd.getNumPartitions) // 1
    //Coalesce,
    dfJson.coalesce(2)
    println(dfJson.rdd.getNumPartitions) // 1




  }
  
}