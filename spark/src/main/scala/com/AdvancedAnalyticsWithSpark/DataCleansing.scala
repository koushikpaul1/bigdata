package com.AdvancedAnalyticsWithSpark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object DataCleansing {


  def parsing(spark: SparkSession): DataFrame = {
    val parsed = spark.read.
      option("header", "true").
      option("nullValue", "?").
      option("inferSchema", "true").
      csv("input/Advanced-Analytics-with-Spark/linkage")
    parsed
  }

  def tranpose(parsed: DataFrame): DataFrame = {
    //parsed.createOrReplaceTempView("linkage")
    val summary = parsed.describe()
    val schema = summary.schema
    import parsed.sparkSession.implicits._
    val longForm = summary.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
        (metric, schema(i).name, row.getString(i).toDouble)
      })
    })
    val longDF = longForm.toDF("metric", "field", "value")
    val wideDF = longDF.groupBy("field").pivot("metric").agg(first("value"))
   // wideDF.show()
    wideDF
  }

  def main(arg: Array[String]): Unit = {
    //sparkContext
    val spark = SparkSession
      .builder
      .appName("DataCleansing")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    val parsed = parsing(spark)
    parsed.cache()

    val matches = parsed.where("is_match = true")
    val misses = parsed.where("is_match = false")// val misses = parsed.filter($"is_match" === false)
    tranpose(matches).createOrReplaceTempView("match_view")
    tranpose(misses).createOrReplaceTempView("miss_view")
    spark.sql("""SELECT a.field, a.count + b.count total, a.mean - b.mean delta FROM match_view a INNER JOIN miss_view b ON a.field = b.field WHERE a.field NOT IN ("id_1", "id_2") ORDER BY delta DESC, total DESC """).show()
  }

  def sparkContext() = {
    val sc = new SparkContext("local[*]", "DataCleansing")
    val rawblocks = sc.textFile("input/Advanced-Analytics-with-Spark/linkage")
    println(rawblocks.getNumPartitions) //2
    println(rawblocks.first) //id_1","id_2","cmp_fname_c1","cmp_fname_c2","cmp_lname_c1","cmp_lname_c2","cmp_sex","cmp_bd","cmp_bm","cmp_by","cmp_plz","is_match"
  }
}
