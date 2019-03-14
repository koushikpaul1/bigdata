package com.edge.old.streaming.twitter.unstructured

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import com.edge.old.streaming.Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

object C_ThreadSafeAverageTweetLength {
  def main(args: Array[String]) {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    setupLogging()
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    val lengths = statuses.map(status => status.length())
    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)
    var longestTweet = 0
    lengths.foreachRDD((rdd, time) => {
      var count = rdd.count()
    /*  if (count > 0) {
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x, y) => x + y))
        println("Total tweets: " + totalTweets.get() +
          " Total characters: " + totalChars.get() +
          " Average: " + totalChars.get() / totalTweets.get())
      }*/
       if (count > 0) {if(rdd.first()>longestTweet){longestTweet=rdd.first()}
        totalTweets.getAndAdd(count)
        totalChars.getAndAdd(rdd.reduce((x, y) => x + y))
        println("Total tweets: " + totalTweets.get() +
             " Longest Tweet: " + longestTweet +
          " Total characters: " + totalChars.get() +
          " Average: " + totalChars.get() / totalTweets.get())}
    })
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}