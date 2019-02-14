package com.edge.sql

import com.edge.dataFrameNSet.SparkSQL.mapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object HiveExample {

  def main (a: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
 /*   val sc = new SparkContext("local[*]", "HiveExample")
    val hiveCtx= new HiveContext(sc)
    val input = hiveCtx.jsonFile("input/book/testtweet.json")
    input.registerTempTable("tweets")
    val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM       tweets ORDER BY retweetCount LIMIT 10")
    println(topTweets)*/


  val ss = SparkSession
    .builder()
    .master("local[*]")
    .appName("HiveExample")
    .config("spark.sql.warehouse.dir", "file:///D:/temp/spark/hive")
    //.enableHiveSupport()
    .getOrCreate()



    val input = ss.read.json("input/book/testweet.json")
    input.createOrReplaceGlobalTempView("tweets")
    val topTweets = ss.sql("SELECT text, retweetCount FROM       tweets ORDER BY retweetCount LIMIT 10")
    println(topTweets)









  }
}
