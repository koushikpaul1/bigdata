package com.edge.E_template

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import scala.io.Source
import org.apache.spark.storage.StorageLevel
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
    
    
    
    
    

    val sscTwitter = new StreamingContext("local[*]", "Template", Seconds(1)) //DStream object
    sscTwitter.checkpoint("D:/temp/spark/twitter/checkpoint")
    for (line <- Source.fromFile("properties/twitter.txt").getLines)
      System.setProperty("twitter4j.oauth." + line.split(" ")(0), line.split(" ")(1))

    val tweets = TwitterUtils.createStream(sscTwitter, None)
    tweets.map(stream => stream.getText()).print(100)
    sscTwitter.start()
    //ssc.awaitTermination()
    sscTwitter.awaitTerminationOrTimeout(2000)

    
    
    
    
    
    //start ncat on port 9999 feeding from a log file/folder =>ncat -kl 9999 < access_log.txt
    val sscTCP = new StreamingContext("local[*]", "LogParser", Seconds(1))
    val dStream = sscTCP.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    dStream.print(100)
    sscTCP.checkpoint("D:/temp/spark/twitter/checkpoint")
    sscTCP.start()
    sscTCP.awaitTermination()
  }

}