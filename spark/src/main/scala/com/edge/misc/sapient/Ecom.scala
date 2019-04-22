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
 
      dfEcomCompetitor.createOrReplaceTempView("rival")
    dfProduct.createOrReplaceTempView("product")
    dfSeller.createOrReplaceTempView("seller")
    
    
    //spark.sql("""select p.ProductId , p.procuredValue ,p.minMargin, p.maxMargin,now() as TimeStamp, r.rivalName, r.price, now() from product p left join rival r ON p.ProductId=r.productId where p.ProductId='10666136'""").show
     //spark.sql("""select * from (select p.ProductId , p.procuredValue ,p.minMargin, p.maxMargin,now() as TimeStamp, r.rivalName, r.price, now(), rank() over ( partition by P.ProductId order by r.price desc) as rank  from product p left join rival r ON p.ProductId=r.productId ) a where a.rank=1""").show
 
     
  spark.sql("""select b.ProductId,   
  case 
  when (b.procuredValue  +  b.maxMargin) < b.price then (b.procuredValue  +  b.maxMargin) 
  when (b.procuredValue  +  b.minMargin) < b.price then b.price
  when ((b.procuredValue  < b.price) AND b.saleEvent ='Special' )then b.price
  when  b.price < b.procuredValue then 'JaBaba'
  ELSE b.procuredValue  END  as final_Price, now() as TimeStamp, b.price as CheapestPriceamongstallRivals , b.rivalName FROM
  (select * from (select p.ProductId , p.procuredValue ,p.minMargin, p.maxMargin,r.rivalName, r.price,r.saleEvent, now(), 
  rank() over ( partition by P.ProductId order by r.price asc) as rank  from product p left join rival r ON p.ProductId=r.productId ) a where a.rank=1)b """).show  
    
	
	
}