package com.edge.dataFrameNSet

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
object PopularMovies {

  def loadMovieNames(): Map[Int, String] = {
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
  case class Movie(ID: Int)
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "file:///D:/temp/spark")
      .master("local[*]")
      .appName("PopularMovies")
      .getOrCreate()
    val nameMap = spark.sparkContext.broadcast(loadMovieNames)

    val lines = spark.sparkContext.textFile("data/ml-100k/u.data")
    val movieId = lines.map(x => Movie(x.split("\t")(0).toInt))

    import spark.implicits._

    val movieIdDS = movieId.toDS()
    movieIdDS.groupBy("ID").count().orderBy(desc("count")).cache.show()
    val top10 = movieIdDS.groupBy("ID").count().orderBy(desc("count")).take(10)
    for (result <- top10) {
      println(loadMovieNames()(result(0).asInstanceOf[Int]) + ": " + result(1))
    }

    /* val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    val movieCounts = movies.reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
    val movieCountsName = movieCounts.map(x => (nameMap.value(x._1), x._2))
    movieCountsName.foreach(println)*/
  }
}


