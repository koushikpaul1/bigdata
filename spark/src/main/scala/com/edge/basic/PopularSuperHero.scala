package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source

object PopularSuperHero {

  def groupFriends(line: String) = {
    def names = line.split("\\s+")
    (names(0).toInt, names.length - 1)
  }

  def nameMap(line: String): Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None
    }
  }

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[1]", "MostPopularSuperhero")
    val namesRdd = sc.textFile("input/course/marvel-names.txt").flatMap(nameMap)
    val friends = sc.textFile("input/course/marvel-graph.txt").map(groupFriends).reduceByKey((x, y) => (x + y)).map(x => (x._2, x._1)).sortByKey(false)
    println(namesRdd.lookup(friends.first()._2)(0) + " is the most popular superhero with " + friends.first()._1 + " co-apperances")
  }
}