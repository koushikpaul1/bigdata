package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
//import breeze.linalg.split
import org.apache.spark.SparkConf

object MovieSimilarities {
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  def loadMovieNames(): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var movieMap: Map[Int, String] = Map()
    val lines = Source.fromFile("data/ml-100k/u.item").getLines()
    for (line <- lines) {
      val fields = line.split('|')
      if (fields.length > 1) movieMap += (fields(0).toInt -> fields(1))
    }
    return movieMap
  }
  def makePairs(userRatings: UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2
    ((movie1, movie2), (rating1, rating2))
  }
  def isDuplicate(userRatings: UserRatingPair): Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    return movie1 < movie2
  }
  def createTuple(line: String) = {
    val values = line.split("\t")
    (values(0).toInt -> (values(1).toInt, values(2).toDouble))
  }
  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    val numerator: Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    return (score, numPairs)
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    println("\nLoading movie names...")
    val nameDict = loadMovieNames()
    val data = sc.textFile("data/ml-100k/u.data")
    val ratings = data.map(createTuple)
    val joinedRatings = ratings.join(ratings)
    val uniqueJoinedRatings = joinedRatings.filter(isDuplicate)
    val moviePairs = uniqueJoinedRatings.map(makePairs)
    val moviePairRatings = moviePairs.groupByKey()
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0
      val movieID: Int = args(0).toInt
      val filteredResults = moviePairSimilarities.filter(x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        })
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(false).take(10)
      println("\nTop 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
}