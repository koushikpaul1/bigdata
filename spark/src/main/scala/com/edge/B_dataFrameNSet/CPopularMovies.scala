package com.edge.B_dataFrameNSet

import org.apache.spark._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.apache.spark.sql.functions._
object CPopularMovies {
  case class ID(id: Int)
  def loadMovieNames(): Map[Int, String] = {
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
    val spark = SparkSession.
      builder.
      appName("CPopularMovies").
      master("local").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
    val movieMap = spark.sparkContext.broadcast(loadMovieNames)
    val movieRDD = spark.sparkContext.textFile("input/udemy/spark-scala/ml-100k/u.data")
    val movieCase = movieRDD.map(record => ID(record.split("\t")(1).toInt))
    import spark.implicits._
    val movieDS = movieCase.toDS()
    
    val topMovieIds = movieDS.groupBy("id").count().orderBy(desc("count")).cache
    val movieArray=topMovieIds.take(10)
   
    for (movie <- movieArray){
      println(loadMovieNames()(movie(0).asInstanceOf[Int])+" : "+movie(1))
    }
    
    
    
    val movieNameRDD = spark.sparkContext.broadcast(loadMovieNames)
     println("\n\n*********************\n\n"+movieNameRDD.value(topMovieIds.first()(0).asInstanceOf[Int]))
    spark.stop()
  }

}