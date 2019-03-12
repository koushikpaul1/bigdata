package com.edge.dataFrameNSet

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object ADataSet {
  case class Person(ID: Int, name: String, age: Int, numFriends: Int)
  def mapper(line: String): Person = {
    val fields = line.split(',')
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.
      builder.
      appName("Template").
      master("local").
      config("spark.sql.warehouse.dir", "file:///D:/temp/spark"). // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      getOrCreate()
    val linesRDD = spark.sparkContext.textFile("input/udemy/spark-scala/fakefriends.csv")
    val peopleCase = linesRDD.map(mapper)
    import spark.implicits._
    val peopleDS = peopleCase.toDS
    peopleDS.printSchema()
    peopleDS.createOrReplaceTempView("people")
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    val results = teenagers.collect()
    results.foreach(println)
    spark.stop()
  }

}