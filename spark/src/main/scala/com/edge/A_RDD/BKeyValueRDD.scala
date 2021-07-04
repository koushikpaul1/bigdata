package com.edge.A_RDD

import org.apache.log4j._
import org.apache.spark._
//keyValue RDD can be created from a map function

object BKeyValueRDD {
  
  def parseLine (line:String)={
    val fields= line.split(",")
    (fields(2).toInt,fields(3).toInt)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc =new SparkContext("local[2]","BKeyValueRDD")
    val linesRDD=sc.textFile("input/udemy/spark-scala/fakefriends.csv", 5)
    val ageNumFrndRDD=linesRDD.map(parseLine)
     
    val unsortedResult=ageNumFrndRDD.
    mapValues(data => (data,1)). // key/value RDD to key/complex value(tuple) RDD
    reduceByKey((x,y)=>(x._1+y._1,x._2+y._2)).
    mapValues(x=> (x._1 / x._2))
    
    val result=unsortedResult.sortByKey(true)// sortByKey works on key/value RDD
    result.foreach(println)// This doesn't give a global sort , because we have 5 partitions
    
    val result2=unsortedResult.collect()// this returns an array
    result2.sorted.foreach(println)//sorted works on Array, as the result was collected already , this gives a global sort
     
  }
  
}