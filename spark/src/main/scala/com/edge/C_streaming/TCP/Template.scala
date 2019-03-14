package com.edge.C_streaming.TCP

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object Template {
  def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)

 

      
      
  
val conf = new SparkConf().setAppName("Template").setMaster("local[*]")
val ssc = new StreamingContext(conf, Seconds(1))
 val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)   
    //val words = lines.flatMap(_.split(" "))
   // val pairs = words.map(word => (word, 1))
    //val wordCounts = pairs.reduceByKey(_ + _)
   // wordCounts.print()
 lines.print()
    ssc.start()         
ssc.awaitTermination()
  }
  
}