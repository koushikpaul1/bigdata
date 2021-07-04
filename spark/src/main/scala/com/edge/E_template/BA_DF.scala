package com.edge.E_template

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ min, max, avg }
import org.apache.spark.sql.functions.{expr, col, column}

object DF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.
    builder().
    appName("DF").
    master("local[2]").
    config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    getOrCreate()

    
    
  import spark.implicits._
  import spark.sql

  //1 .Create DataFrame from range
  val dfMyRange = spark.range(10010).toDF("number")
  val divisBy2 = dfMyRange.where("number % 2 = 0")
  println(divisBy2.count()) //500
  dfMyRange.createOrReplaceTempView("dfTableRange")

  
  
  
  //2 .Create DataFrame with Json( json has embeded schema)
  val dfJson = spark.read.json("input/Spark-The-Definitive-Guide/flight-data/json/2015-summary.json")
  val jsonSchema = dfJson.schema
  println(jsonSchema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,LongType,true))
  dfJson.createOrReplaceTempView("dfTableJson")

  
  
  
  //3 .Create DataFrame with CSV without schema
  val dfCSV = spark
    .read.format("csv")
    .load("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfCSV.schema) //StructType(StructField(_c0,StringType,true), StructField(_c1,StringType,true), StructField(_c2,StringType,true))
  dfCSV.createOrReplaceTempView("dfTableCSV")

  
  
  
  //4 .Create DataFrame with infer schema
  val dfCSVInferSchema = spark
    .read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfCSVInferSchema.schema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,IntegerType,true))
  dfCSVInferSchema.createOrReplaceTempView("dfTableCSVInfer")

  
  
  
  //5 .Create DataFrame with own schema
  import org.apache.spark.sql.types._ 
  val myManualSchema = new StructType(Array(
    new StructField("DEST_COUNTRY_NAME", StringType, true),
    new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    new StructField("count", LongType, false)))
  val dfJsonWithSchema = spark.read.schema(myManualSchema).csv("input/Spark-The-Definitive-Guide/flight-data/csv/2015-summary.csv")
  println(dfJsonWithSchema.schema) //StructType(StructField(DEST_COUNTRY_NAME,StringType,true), StructField(ORIGIN_COUNTRY_NAME,StringType,true), StructField(count,LongType,true))
  dfJsonWithSchema.createOrReplaceTempView("dfTableCSVSchema")

  
  
  
  //6 .Create DataFrame from table
  import org.apache.spark.sql._
  val dfMysqlEmp = spark.
    read.
    format("jdbc").
    option("url", "jdbc:mysql://localhost/employees?useSSL=false").
    option("driver", "com.mysql.jdbc.Driver").
    option("dbtable", "employees").
    option("user", "root").
    option("password", "root").
    load()
  println(dfMysqlEmp.schema) //StructType(StructField(emp_no,IntegerType,true), StructField(birth_date,DateType,true), StructField(first_name,StringType,true), StructField(last_name,StringType,true), StructField(gender,StringType,true), StructField(hire_date,DateType,true))
  //dataframe_employees.show(10)
  dfMysqlEmp.createOrReplaceTempView("dfTableEmp")

  // from TempView
  sql("select * from dfTableEmp e join dfTableRange r on e.emp_no=r.number").show(5)  
  
  
  //from DataFrame
  dfMysqlEmp.show(5)
  //As String
  dfMysqlEmp.select("emp_no").show(5)
  dfMysqlEmp.select("emp_no", "first_name").show(5)
  //As Column
  dfMysqlEmp.select(col("emp_no"), col("first_name")).show(5)
  dfMysqlEmp.select(dfMysqlEmp.col("emp_no"), dfMysqlEmp.col("first_name")).show(5)
  dfMysqlEmp.select(column("emp_no"), column("first_name")).show(5)
  dfMysqlEmp.select('emp_no, 'first_name).show(5)
  dfMysqlEmp.select($"emp_no", $"first_name").show(5)
  dfMysqlEmp.select(expr("emp_no"), expr("first_name")).show(5)
  dfMysqlEmp.selectExpr("emp_no", "first_name").show(5)
  dfMysqlEmp.selectExpr("emp_no as FATAKESTO", "first_name").show(5)
  dfMysqlEmp.select(col("first_name"), dfMysqlEmp.col("first_name"), column("first_name"), 'first_name, $"first_name", expr("first_name")).show(5)// cant mix String with Column

  // treating DF as RDD
  dfMysqlEmp.map(emp => "EmpNo " + emp.getAs[String]("emp_no")).show(5)
  dfMysqlEmp.map(emp => "EmpNo " + emp(0)).show(5)
   
 
  //adding a column
  import org.apache.spark.sql.functions.lit
  dfMysqlEmp.select(expr("*"), lit("Koushik")as ("edge")).show(5)
   

}
