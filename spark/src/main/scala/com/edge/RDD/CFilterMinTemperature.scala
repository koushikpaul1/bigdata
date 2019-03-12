package com.edge.RDD

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.math.min

object CFilterMinTemperature {
  def parseData(line:String)={
    val fields=line.split(",")
    (fields(0),fields(2),fields(3).toFloat)
  }
  
  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[4]", "Template")
    val linesRDD=sc.textFile("input/udemy/spark-scala/1800.csv").map(parseData)
    val resultRDD=linesRDD.
    filter(x=>x._2=="TMIN"). // Filtering on the basis of a field value
    map(x=>(x._1,x._3)).     // Filtering out a column    
    reduceByKey((x,y)=> min(x,y)) // converting to a keyValue RDD and applying min function on values(two at a time)
    val resultArray=resultRDD.collect()
    for(result<-resultArray.sorted){
      val station = result._1
      val temp = result._2
      println(s"stationID $station Temperature $temp")
    }
  }

}