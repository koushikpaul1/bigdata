/*package com.edge.C_streaming.kafka

import org.apache.spark.sql.SparkSession
import com.edge.C_streaming.TCP.Utilities._
import org.apache.log4j._
import org.apache.spark.sql.{SparkSession
//import org.apache.spark.annotation.InterfaceStability
  
object SparkKafkaIntegration {
  def main(abc: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

      val streamingDataFrame = spark.read.
      option("header", "true").
      option("nullValue", "?").
      option("inferSchema", "true").
      csv("input/kafka")

    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "topicName")
      .option("kafka.bootstrap.servers", "192.168.85.133:9092")
      .option("checkpointLocation", "path to your local dir")
      .start()
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.85.133:9092")
      .option("subscribe", "locator")
      .load()

    val df1 = df.select("status", "dateTime")
    df1.collect()
    //val personEncoder = Encoders.bean(Bay.class) 
  //  val ds1=df.as[Bay](personEncoder)
    //val windowed =  df1.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1)) 
//val row=
    df1.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
*/