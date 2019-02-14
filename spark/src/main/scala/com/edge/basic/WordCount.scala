package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object WordCount {

  def main(a: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[1]", "WordCount")

    val input=sc.textFile("input/course/Book.txt", 5)
    //val words=input.flatMap(x=> x.split(" "))
    val words = input.flatMap(x => x.split("\\W+"))
    val lowercaseWords = words.map(x => x.toLowerCase())
    // val wordCounts=words.countByValue()
    //val wordCounts=lowercaseWords.countByValue()
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()
    val wordCountsSortedInverted = wordCountsSorted.map(x => (x._2, x._1))
    //wordCounts.foreach(println)
    // wordCountsSorted.foreach(println)
    wordCountsSortedInverted.foreach(println)
  }
}