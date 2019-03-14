package com.edge.A_RDD

import org.apache.spark._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.log4j._

object EBroadCastVariablePopularMovies {
  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieMap: Map[Int, String] = Map()
    val lines = Source.fromFile("input/udemy/spark-scala/ml-100k/u.item").getLines() //another way of reading from file without SparkContext/SparkSession
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 0) movieMap += (fields(0).toInt -> fields(1)) // creating a Map (key/value)
    }
    movieMap
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[4]", "EPopularMovies")
    val movieMap=sc.broadcast(loadMovieNames)// creating and broadcasting a value
    val lines=sc.textFile("input/udemy/spark-scala/ml-100k/u.data")
    val result=lines.
    map(line => (line.split("\t")(1),1)).
    reduceByKey((x,y)=>x+y).
    map(pair=>(pair._2,pair._1)).
    sortByKey(false).
    //map(pair=>(pair._2,pair._1)).
    map(tuple=>(movieMap.value((tuple._2).toInt),tuple._1))
    result.foreach(println)
    
  }

}