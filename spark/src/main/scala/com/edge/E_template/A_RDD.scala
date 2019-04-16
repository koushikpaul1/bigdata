package com.edge.E_template

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.log4j._

object RDD {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.
      builder.
      appName("RDD").
      master("local[*]").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
    println("sparkSession created")
    val sc = spark.sparkContext
    val sc2 = new SparkContext("local[*]", "RDD")

    //from Parallelized collection
    val rdd0 = sc.parallelize("Spark The Definitive Guide ".split(" "), 2) //
    val rdd1 = sc.parallelize(1 to 10, 2)
    rdd1.foreach(println)
    println()
    val rdd2 = sc.parallelize(Array("jan", "feb", "mar", "april", "may", "jun"), 3)
    val result = rdd2.coalesce(2) //Return a new RDD that is reduced into numPartitions partitions.This results in a narrow dependency, e.g. if you go from 1000 partitions to 100 partitions, there will not be a shuffle, instead each of the 100 new partitions will claim 10 of the current partitions. If a larger number of partitions is requested, it will stay at the current number of partitions.
    result.foreach(println)
    println()
    val rdd3 = sc.parallelize(Seq(("Edge", 52), ("Jhon", 15), ("Katara", 42), ("Koustav", 65), ("Raju", 35)), 1)
    val rdd33 = sc.parallelize(Seq(("Hemant", 15, 2000), ("Koustav", 65, 4000), ("Edge", 52, 1000), ("Raju", 35, 5000), ("Katara", 42, 3000)), 1)
    val rdd3Sorted = rdd3.sortByKey()
    //val rdd33Sorted = rdd33.sortByKey()//won't compile because its not a key value pair
    rdd3Sorted.foreach(println)
    println()

    //from data storage
    val textRDD1 = sc.textFile("input/udemy/spark-scala/ml-100k/u.data", 5)
    val textRDD2 = spark.read.textFile("input/udemy/spark-scala/ml-100k/u.data").rdd
    val jsonRDD = spark.read.json("path/of/json/file").rdd.persist() // if used persist it will not be recomputed again rather will be stored in mem and disk
  }

}