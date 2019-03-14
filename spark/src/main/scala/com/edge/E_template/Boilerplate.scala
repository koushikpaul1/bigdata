package com.edge.E_template

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import scala.io.Source

object Boilerplate {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.
      builder.
      appName("Template").
      master("local").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
    println("SparkSession created")
    spark.stop()

    val sc = new SparkContext("local[4]", "Template")
    println("SparkContext created")
    sc.stop()
    
    val sc2 = spark.sparkContext
    println("SparkContext created from SparkSession")
    sc.stop()
    
    val ssc = new StreamingContext("local[*]", "Template", Seconds(1))
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    for (line <- Source.fromFile("properties/twitter.txt").getLines)
    System.setProperty("twitter4j.oauth." + line.split(" ")(0), line.split(" ")(1))
    val tweets = TwitterUtils.createStream(ssc, None)
     tweets.map(stream => stream.getText()).print(100)
    ssc.start()
    //ssc.awaitTermination()
    ssc.awaitTerminationOrTimeout(2000)
  }

}