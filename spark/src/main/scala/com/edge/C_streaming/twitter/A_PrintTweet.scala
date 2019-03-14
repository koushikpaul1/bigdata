package com.edge.C_streaming.twitter

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import scala.io.Source
object C_PrintTweet2 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "APrintTweet", Seconds(1))

    val tweetStream = TwitterUtils.createStream(ssc, None)//This is a DStream not an RDD
    tweetStream.map(stream => stream.getText()).print(100)
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

