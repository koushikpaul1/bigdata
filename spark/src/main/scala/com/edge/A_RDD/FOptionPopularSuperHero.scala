package com.edge.A_RDD

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
object FOptionPopularSuperHero {
  
  def nameMap(line:String):Option[(Int,String)]={
var fields = line.split('\"')
    if (fields.length > 1) return Some(fields(0).trim().toInt, fields(1))
    else return None 
  } 
   def main(args: Array[String]) {
      val sc =new SparkContext("local[4]","FOptionPopularSuperHero")
      val namesRDD = sc.textFile("input/udemy/spark-scala/marvel-names.txt").flatMap(nameMap)
      val friendsRDD = sc.textFile("input/udemy/spark-scala/marvel-graph.txt").
      map(line=>(line.split("\\s+")(0),line.split("\\s+").length-1)).
      reduceByKey((x,y)=>x+y).
     // map(pair=>(pair._2,pair._1)).
     // sortByKey(false,1)
      sortBy(line => line._2,false)
      println(namesRDD.lookup((friendsRDD.first()._2.toInt))(0)+" is the most popular Hero")
  }  
}