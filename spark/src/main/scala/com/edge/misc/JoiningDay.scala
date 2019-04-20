package com.edge.misc

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions.{ min, max, avg, count }

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
  val dataframe_employees = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/employees?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "employees").option("user", "root").option("password", "root").load()
  val dataframe_customers = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "customers").option("user", "root").option("password", "root").load()
  dataframe_customers.createOrReplaceTempView("customers")
  val dataframe_products = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "products").option("user", "root").option("password", "root").load() 
  dataframe_products.createOrReplaceTempView("products")
  val dataframe_orders = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "orders"). option("user", "root").option("password", "root").load()
  val dataframe_order_items = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "order_items").option("user", "root").option("password", "root").load()
  dataframe_order_items.createOrReplaceTempView("order_items")
  val dataframe_order_items_all = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost/retail_db?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "order_all").option("user", "root").option("password", "root").load()
  dataframe_order_items_all.createOrReplaceTempView("order_all")
  //dataframe_orders.show(10)

  // dataframe_employees.select("gender").show(10)
  // dataframe_employees.groupBy("gender").count().show()
  //dataframe_products.groupBy("product_category_id").agg(min("product_price"), max("product_price"), avg("product_price")).show

  // sql("select  p.product_price , oi.order_item_subtotal  from order_items oi join products p limit 10").show()

  //sql("select * from products limit 10").show
  //dataframe_order_items_all.groupBy
  // sql("select category,item_quantity,count(price),min(price),max(price) as max,avg(price) from order_all  group by category,item_quantity").show()
  // dataframe_order_items_all.groupBy("category","item_quantity").agg(count("price"),min("price"), max("price") as "max", avg("price")).show()

  val filteredCustomers1 = dataframe_customers.filter("customer_state=='TX'").filter("customer_zipcode LIKE '770%%'").filter("customer_city !='Dallas' AND customer_city !='Carrollton'")
  val filteredCustomers2 = dataframe_customers.where("customer_state=='TX'").where("customer_zipcode LIKE '770%%'").where("customer_city !='Dallas' AND customer_city !='Carrollton'")
  val filteredCustomers3 = sql("select * from customers p where p.customer_state ='TX' and p.customer_city not in ('Carrollton','Dallas') and customer_zipcode like ('770%%')")

  //extraneous input '&' expecting {'(', 'SELECT', 'FROM', 'ADD', 'AS', 'ALL', 'ANY', 'DISTINCT', 'WHERE', 'GROUP', 'BY', 'GROUPING', 'SETS', 'CUBE', 'ROLLUP', 'ORDER', 'HAVING', 'LIMIT', 'AT', 'OR', 'AND', 'IN', NOT, 'NO', 'EXISTS', 'BETWEEN', 'LIKE', RLIKE, 'IS', 'NULL', 'TRUE', 'FALSE', 'NULLS', 'ASC', 'DESC', 'FOR', 'INTERVAL', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'JOIN', 'CROSS', 'OUTER', 'INNER', 'LEFT', 'SEMI', 'RIGHT', 'FULL', 'NATURAL', 'ON', 'PIVOT', 'LATERAL', 'WINDOW', 'OVER', 'PARTITION', 'RANGE', 'ROWS', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT', 'FIRST', 'AFTER', 'LAST', 'ROW', 'WITH', 'VALUES', 'CREATE', 'TABLE', 'DIRECTORY', 'VIEW', 'REPLACE', 'INSERT', 'DELETE', 'INTO', 'DESCRIBE', 'EXPLAIN', 'FORMAT', 'LOGICAL', 'CODEGEN', 'COST', 'CAST', 'SHOW', 'TABLES', 'COLUMNS', 'COLUMN', 'USE', 'PARTITIONS', 'FUNCTIONS', 'DROP', 'UNION', 'EXCEPT', 'MINUS', 'INTERSECT', 'TO', 'TABLESAMPLE', 'STRATIFY', 'ALTER', 'RENAME', 'ARRAY', 'MAP', 'STRUCT', 'COMMENT', 'SET', 'RESET', 'DATA', 'START', 'TRANSACTION', 'COMMIT', 'ROLLBACK', 'MACRO', 'IGNORE', 'BOTH', 'LEADING', 'TRAILING', 'IF', 'POSITION', 'EXTRACT', '+', '-', '*', 'DIV', '~', 'PERCENT', 'BUCKET', 'OUT', 'OF', 'SORT', 'CLUSTER', 'DISTRIBUTE', 'OVERWRITE', 'TRANSFORM', 'REDUCE', 'SERDE', 'SERDEPROPERTIES', 'RECORDREADER', 'RECORDWRITER', 'DELIMITED', 'FIELDS', 'TERMINATED', 'COLLECTION', 'ITEMS', 'KEYS', 'ESCAPED', 'LINES', 'SEPARATED', 'FUNCTION', 'EXTENDED', 'REFRESH', 'CLEAR', 'CACHE', 'UNCACHE', 'LAZY', 'FORMATTED', 'GLOBAL', TEMPORARY, 'OPTIONS', 'UNSET', 'TBLPROPERTIES', 'DBPROPERTIES', 'BUCKETS', 'SKEWED', 'STORED', 'DIRECTORIES', 'LOCATION', 'EXCHANGE', 'ARCHIVE', 'UNARCHIVE', 'FILEFORMAT', 'TOUCH', 'COMPACT', 'CONCATENATE', 'CHANGE', 'CASCADE', 'RESTRICT', 'CLUSTERED', 'SORTED', 'PURGE', 'INPUTFORMAT', 'OUTPUTFORMAT', DATABASE, DATABASES, 'DFS', 'TRUNCATE', 'ANALYZE', 'COMPUTE', 'LIST', 'STATISTICS', 'PARTITIONED', 'EXTERNAL', 'DEFINED', 'REVOKE', 'GRANT', 'LOCK', 'UNLOCK', 'MSCK', 'REPAIR', 'RECOVER', 'EXPORT', 'IMPORT', 'LOAD', 'ROLE', 'ROLES', 'COMPACTIONS', 'PRINCIPALS', 'TRANSACTIONS', 'INDEX', 'INDEXES', 'LOCKS', 'OPTION', 'ANTI', 'LOCAL', 'INPATH', STRING, BIGINT_LITERAL, SMALLINT_LITERAL, TINYINT_LITERAL, INTEGER_VALUE, DECIMAL_VALUE, DOUBLE_LITERAL, BIGDECIMAL_LITERAL, IDENTIFIER, BACKQUOTED_IDENTIFIER}

dataframe_customers.show()
  
  
  dataframe_customers.where("customer_state=='TX'   AND customer_zipcode LIKE '770%%'")
  
  
  
  
  
  
  
  
  
}