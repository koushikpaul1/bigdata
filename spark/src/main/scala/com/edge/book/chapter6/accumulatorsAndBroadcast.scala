package com.edge.book.chapter6

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.SparkContext._

//import org.eclipse.jetty.client.ContentExchange
//import org.eclipse.jetty.client.HttpClient

object accumulatorsAndBroadcast {

  def main( a : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc =new SparkContext("local","accumulatorsAndBroadcast" )
    val file = sc.textFile("input/book/callsigns")
    val blankLines = sc.accumulator(0)
    val callSigns = file.flatMap(line =>{
      if(line=="")
        {blankLines+=1}
      line.split(" ")
    })


    callSigns.saveAsTextFile("output/book/chapter6/accumulatorsAndBroadcast")
    callSigns.foreach(println)
    println("blankLines "+blankLines)
  }

}
