package com.edge.streaming.twitter.unstructured

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import com.edge.streaming.Utilities._

object PrintTweets {
  def main(args: Array[String]) {
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))
    setupLogging()
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    statuses.print(100)
    ssc.start()
    ssc.awaitTermination()
  }
}