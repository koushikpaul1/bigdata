package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BC_Aggregation extends App{
  println("BC_Aggregation")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark=SparkSession.builder().appName("BC_Aggregation").master("local[*]").getOrCreate()
  val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("input/Spark-The-Definitive-Guide/retail-data/all/*.csv").coalesce(5)
  df.cache()
  df.createOrReplaceTempView("dfTable")
  df.show(5)
  
//count
println(df.count)
df.select(count("*")).show()
df.select(countDistinct("StockCode")).show()
df.select(approx_count_distinct("StockCode", 0.1)).show()

df.select(min("Quantity"), max("Quantity")).show()
df.select(sum("Quantity")).show()
df.select(sumDistinct("Quantity")).show()

val dff=df.select(count("Quantity").alias("total_transactions"),sum("Quantity").alias("total_purchases"),avg("Quantity").alias("avg_purchases"),expr("mean(Quantity)").alias("mean_purchases"))
dff.show()
dff.selectExpr("total_purchases/total_transactions as avg","avg_purchases","mean_purchases").show(5)
df.agg(count("Quantity"),sum("Quantity").alias("total_purchases"),avg("Quantity").alias("avg_purchases"),expr("mean(Quantity)").alias("mean_purchases")).show(5)

  //The variance is the average of the squared differences from the mean, and the standard deviation is the square root of the variance.
  //Spark has both the formula for the "sample standard deviation" as well as the formula for the "population standard deviation". Default is the sample one.
  df.select(variance("Quantity"), stddev("Quantity")).show()
  df.select(var_samp("Quantity"), stddev_samp("Quantity")).show()
  df.select(var_pop("Quantity"), stddev_pop("Quantity")).show()

//skewness and kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
//Covariance and Correlation
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),covar_pop("InvoiceNo", "Quantity")).show()

//collect a list of values present in a given column or only the unique values by collecting to a set.
df.agg(collect_set("Country"), collect_list("Country")).show()

//Grouping
df.groupBy("InvoiceNo", "CustomerId").count().show()
df.groupBy("InvoiceNo").agg(count("Quantity").alias("quan"),expr("count(Quantity)")).show()
df.groupBy("InvoiceNo").agg("Quantity"->"avg", "Quantity"->"stddev_pop").show()

//window
import org.apache.spark.sql.expressions.Window
val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")
val windowSpec = Window.partitionBy("CustomerId", "date").orderBy(col("Quantity").desc).rowsBetween(Window.unboundedPreceding, Window.currentRow)
val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

val purchaseDenseRank = dense_rank().over(windowSpec)
val purchaseRank = rank().over(windowSpec)
dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId").select(col("CustomerId"),col("date"),col("Quantity"),purchaseRank.alias("quantityRank"),purchaseDenseRank.alias("quantityDenseRank"),maxPurchaseQuantity.alias("maxPurchaseQuantity")).show()




}