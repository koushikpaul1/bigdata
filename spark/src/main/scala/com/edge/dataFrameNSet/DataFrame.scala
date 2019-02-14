package com.edge.dataFrameNSet

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object DataFrame {

  case class Person(ID: Int, name: String, age: Int, numFriends: Int)
  def mapper(line: String): Person = {
    val fields = line.split(',')
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }

  def main(a: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession
      .builder
      .config("spark.sql.warehouse.dir", "file:///D:/temp/spark")
      .master("local[*]")
      .appName("DataFrame")
      .getOrCreate()
    import ss.implicits._
    val people = ss.sparkContext.textFile("input/course/fakefriends.csv").map(mapper).toDS().cache()
    println("Here is our inferred schema:")
    people.printSchema()
    println("Let's select the name column:")
    people.select("name").show()
    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()
    println("Group by age:")
    people.groupBy("age").count().show()
    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") + 10).show()
    ss.stop()
  }
}