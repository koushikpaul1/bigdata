package com.edge.misc

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions.{ min, max, avg }

object JoiningDay extends App {
  println("JoiningDay")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.
    builder.
    appName("JoiningDay").
    master("local").
    config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    getOrCreate()

  import spark.implicits._
  import spark.sql
  val dataframe_employees = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/employees?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "employees").
    option("user", "root").
    option("password", "root").
    load()
  //dataframe_employees.show(10)
  val dataframe_customers = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "customers").
    option("user", "root").
    option("password", "root").
    load()
  //dataframe_customers.show(10)
  val dataframe_products = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "products").
    option("user", "root").
    option("password", "root").
    load() 
  //dataframe_products.show(10)
    dataframe_products.createOrReplaceTempView("products")
  val dataframe_orders = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "orders").
    option("user", "root").
    option("password", "root").
    load()
    
    val dataframe_order_items = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "order_items").
    option("user", "root").
    option("password", "root").
    load()
    dataframe_order_items.createOrReplaceTempView("order_items")
  //dataframe_orders.show(10)

 // dataframe_employees.select("gender").show(10)
 // dataframe_employees.groupBy("gender").count().show()
 //dataframe_products.groupBy("product_category_id").agg(min("product_price"), max("product_price"), avg("product_price")).show
  
 // sql("select  p.product_price , oi.order_item_subtotal  from order_items oi join products p limit 10").show()
  
  sql("select * from products limit 10").show
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}