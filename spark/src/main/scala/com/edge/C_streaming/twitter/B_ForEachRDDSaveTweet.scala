package com.edge.C_streaming.twitter
// in a map on RDD , we get every line one after another
// in a foreachRDD on DStream , we get every RDD one after another with the frequency mentioned in streamcontext
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j._
import scala.io.Source
object BForEachRDDSaveTweet {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    setupTwitter()
    val ssc = new StreamingContext("local[*]", "APrintTweet", Seconds(1))

    val tweetDataStream = TwitterUtils.createStream(ssc, None) //DStream object
    val tweetStream = tweetDataStream.map(stream => stream.getText())
    var totalTweets: Long = 1
  var countt: Long=0
    tweetStream.foreachRDD((rdd, time) => {
        countt+=1 
        println("Count "+countt)
      if (rdd.count() > 0) {
        val repartitionedRDD = rdd.repartition(1).cache()
        repartitionedRDD.saveAsTextFile("output/streaming/SaveTweets/Tweets_" + time.milliseconds.toString)
        totalTweets += repartitionedRDD.count()
        println("Tweet count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
        }
      }})

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

