package com.edge.C_streaming.twitter

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import scala.io.Source

object D_PopularHashTag {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    for (line <- Source.fromFile("properties/twitter.txt").getLines)
      System.setProperty("twitter4j.oauth." + line.split(" ")(0), line.split(" ")(1))

    val ssc = new StreamingContext("local[*]", "D_PopularHashTag", Seconds(1))
    val twitterStream = TwitterUtils.createStream(ssc, None)
    twitterStream.
      map(status => status.getText).
      flatMap { text => text.split(" ") }.
      filter { word => word.startsWith("#") }.
      map { tweet => (tweet, 1) }.
      reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(300), Seconds(2)).
      transform(rdd => (rdd.sortBy(x => x._2, false))).
      print()
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    ssc.start()
    ssc.awaitTerminationOrTimeout(10000)
  }

}