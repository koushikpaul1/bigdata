package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMovies {

  def loadMovieNamess(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PopularMoviesNicer")
    val nameMap = sc.broadcast(loadMovieNamess)
    val lines = sc.textFile("data/ml-100k/u.data")
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    val movieCounts = movies.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    val movieCountsName = movieCounts.map(x => (nameMap.value(x._1), x._2))
    movieCountsName.foreach(println)
  }
}


