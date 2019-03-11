package com.edge.basic

// (The file format is userID, movieID, rating, timestamp)
//count only ratings

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object ARatingsCounter {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    sparkContext
  }
  def sparkContext() {
    def sc = new SparkContext("local[*]", "ReadWrite")
    val lineRDD = sc.textFile("input/udemy/spark-scala/ml-100k/u.data", 1)
    val ratingsRDD = lineRDD.map(line => line.split("\t")(2)) // only the third field
    
    val ratingsCountTuple = ratingsRDD.countByValue() // its a tuple/map( when an RDD has only two fields, it can be considered as a tuple=a java map)
    val sortedCount = ratingsCountTuple.toSeq.sortBy(_._1) // convert map to seq to sort by first element of the map/tuple
    
    
    val ratingsSortedCountKeyValue=ratingsRDD.map(x=> (x,1)).reduceByKey((x,y)=>x+y).sortByKey(false)
    
    sortedCount.foreach(println)
    println("\n\n**************\n\n")
    ratingsSortedCountKeyValue.foreach(println)
  }

 /* def sparkSession() {
    val spark = SparkSession.
      builder.
      appName("ARatingsCounter").
      master("local").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()

    val lineDF = spark.read.text("input/udemy/spark-scala/ml-100k/u.data")

  }*/
}

