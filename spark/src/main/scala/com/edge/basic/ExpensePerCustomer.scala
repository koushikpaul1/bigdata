package com.edge.basic

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object ExpensePerCustomer {
  
  def parseLine(line:String)={
    val values=line.split(",")
    (values(0).toInt,values(2).toFloat)
  }
  def main(a: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    def sc= new SparkContext("local[1]","ExpensePerCustomer")  
    def input=sc.textFile("input/course/customer-orders.csv", 5)
    def rdd=input.map(parseLine)
    val result=rdd.reduceByKey((a,b)=>(a+b)).map(c=>(c._2,c._1)).sortByKey(false).map(c=>(c._2,c._1))
    result.foreach(println)
  }  
}