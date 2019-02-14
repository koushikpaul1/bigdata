package com.edge.dataFrameNSet

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  case class Person(ID: Int, name: String, age: Int, numFriends: Int)
  def mapper(line: String): Person = {
    def fields = line.split(',')
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///D:/temp/spark")
      .getOrCreate()

    val people = ss.sparkContext.textFile("input/course/fakefriends.csv").map(mapper)
    import ss.implicits._
    val schemaPeople = people.toDS()
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")
    val teenagers = ss.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    val results = teenagers.collect()
    results.foreach(println)
    ss.stop()
  }
}