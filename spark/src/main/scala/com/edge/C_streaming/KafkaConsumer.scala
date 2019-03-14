package com.edge.C_streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.log4j._

object KafkaConsumer {
  def main(a: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1)) //creating sparkStreamingContext
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.85.133:9094,192.168.85.133:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "locator-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = List("locator").toSet

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val df=stream.map((record => (record.key.getBytes, record.value)))
    println(df.count()) 
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}

