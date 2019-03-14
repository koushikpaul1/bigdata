package com.edge.A_RDD

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object DFlatMapWordCount {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[4]", "DFlatMapWordCount")
    val linesRDD = sc.textFile("input/udemy/spark-scala/book.txt", 5)
    val wordsRDD = linesRDD.
      flatMap(line => line.split("\\W+")).
      map(word => word.toLowerCase()).
      map(lword => (lword, 1)).
      reduceByKey((x, y) => x + y).
      map(x => (x._2, x._1)).
      sortByKey(false, 1).
      map(x => (x._2, x._1))

    val wordsRDD2 = linesRDD.
      flatMap { line => line.split("\\W+") }.
      map(word => word.toLowerCase()).
      countByValue().toSeq.sortBy(_._2) 

    wordsRDD.foreach(println)
    println("\n\n\n****************\n\n\n")
    wordsRDD2.foreach(println)

  }

}