package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

object Template  {
    def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
    
    def main (a: Array[String]){
      Logger.getLogger("org").setLevel(Level.ERROR)
       val sc = new SparkContext("local[*]", "MinTemperatures")
    }
}