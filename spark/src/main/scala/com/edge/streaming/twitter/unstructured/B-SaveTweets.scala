package com.edge.streaming.twitter.unstructured

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import com.edge.streaming.Utilities._

object SaveTweets {
  def main(args: Array[String]) {  
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
      
    val tweets = TwitterUtils.createStream(ssc, None)
    setupLogging()
    val statuses = tweets.map(status => status.getText())
    var totalTweets: Long = 0
    statuses.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) {
        val repartitionedRDD = rdd.repartition(1).cache()
        repartitionedRDD.saveAsTextFile("output/streaming/SaveTweets/Tweets_" + time.milliseconds.toString)
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}