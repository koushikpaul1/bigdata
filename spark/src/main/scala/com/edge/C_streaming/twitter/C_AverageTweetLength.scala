package com.edge.C_streaming.twitter

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import scala.io.Source
object C_AverageTweetLength {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "C-AverageTweetLength", Seconds(1))

    val tweetStream = TwitterUtils.createStream(ssc, None)//This is a DStream not an RDD
    val lengthStream= tweetStream.map(stream => stream.getText().length())
    lengthStream.foreachRDD((rdd,time)=>{
      if(rdd.count>0){
       println("Average tweet length for time "+time+" is "+rdd.reduce((x,y)=>(x+y))/rdd.count)      
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
  def setupTwitter() = {

    for (line <- Source.fromFile("properties/twitter.txt").getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) System.setProperty("twitter4j.oauth." + fields(0), fields(1))
    }
  }
}

