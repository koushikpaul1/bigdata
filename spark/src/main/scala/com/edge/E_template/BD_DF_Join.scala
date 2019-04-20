package com.edge.E_template


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._

object BD_DF_Join extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark =SparkSession.builder().appName("JoiningDay2").master("local[*]").getOrCreate()
  
  
  //val df_employees = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/employees?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "employees").option("user", "root").option("password", "root").load()
  val df_customers = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "customers").option("user", "root").option("password", "root").load()
  val df_products = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "products").option("user", "root").option("password", "root").load() 
  val df_orders = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "orders"). option("user", "root").option("password", "root").load()
  val df_order_items = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "order_items").option("user", "root").option("password", "root").load()
  val df_categories = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "categories").option("user", "root").option("password", "root").load()
  val df_departments = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "departments").option("user", "root").option("password", "root").load()
  val df_order_items_all = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "order_all").option("user", "root").option("password", "root").load()
  
   df_customers.createOrReplaceTempView("customers")
   df_products.createOrReplaceTempView("products")
   df_order_items.createOrReplaceTempView("order_items")
  
  val jeCustOrd=df_customers.col("customer_id")===df_orders.col("order_customer_id")
  val jeOrdItem=df_orders.col("order_id")===df_order_items.col("order_item_order_id")
  val jeProdOrdItem=df_products.col("product_id")===df_order_items.col("order_item_product_id")
  val jeProdCat=df_products.col("product_category_id")===df_categories.col("category_id")
  val jeCatDept=df_categories.col("category_department_id")===df_departments.col("department_id")
  // val jeOrdItem=df_orders.col("")===df_order_items.col("")
  
  
  
  //Get all customers from TX who bought fitness items
  df_customers.where("customer_state = 'TX'").join(df_orders,jeCustOrd).join(df_order_items,jeOrdItem).join(df_products,jeProdOrdItem).join(df_categories,jeProdCat).join(df_departments,jeCatDept).filter("department_name='Fitness'")
  .orderBy("order_id","customer_id").select("customer_id","customer_fname","customer_lname", "customer_street","customer_city","customer_state","customer_zipcode","order_id","category_name","department_name").show(5)
  
  
  
  
  
  
  
}