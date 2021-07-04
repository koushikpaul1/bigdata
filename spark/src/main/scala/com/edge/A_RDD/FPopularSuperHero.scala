package com.edge.A_RDD

import org.apache.spark._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.log4j._

object FPopularSuperHero {
  
  def getNameMap():Map[Int,String]={
    var nameMap: Map[Int,String]=Map() 
    val lines=Source.fromFile("input/udemy/spark-scala/marvel-names.txt").getLines()
    for(line <- lines){
      val fields=line.split("\\s+")
      if(fields.length>1)nameMap+=(fields(0).toInt->fields(1))
    //else{nameMap+=(fields(0).toInt->"XXX")}
      }
    nameMap
  }
   def main(args: Array[String]) {
      val sc =new SparkContext("local[4]","Template")
      val nameMap=sc.broadcast(getNameMap)
      val friends = sc.
      textFile("input/udemy/spark-scala/marvel-graph.txt").
      map(line=>(line.split("\\s+")(0),line.split("\\s+").length-1)).
      reduceByKey((x,y)=>x+y).
      map(pair=>(pair._2,pair._1)).
      sortByKey(false,1).
      map(data=>(nameMap.value((data._2).toInt),data._1))
       friends.foreach(println)  
  }  
}