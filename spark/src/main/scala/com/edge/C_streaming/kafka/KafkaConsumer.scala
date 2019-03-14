package com.edge.C_streaming.kafka

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Seconds, StreamingContext }
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
    // val df=stream.map((record => (record.key.getBytes, record.value)))
    val dStream = stream.map((record => (record.value)))
    dStream.foreachRDD((rdd, time) => //rdd.map(parseData))
      {
        val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
        import sqlContext.implicits._
        val df = rdd.map(parseData).toDF().createOrReplaceTempView("readings")
        sqlContext.sql("select * from readings").show()
        println(rdd.map(parseData))
      })
    dStream.print
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
  def parseData(line: String): Beacon = {
    val fields = line.split(',')
    return Beacon(fields(0).toInt, fields(1), fields(2), fields(3), fields(4).toInt, fields(5).toFloat, 100, fields(7), fields(8), fields(8))
  }
  case class Beacon(id: Int, mac: String, major: String, minor: String, rssi: Int, tx: Float, temp: Float, battery: String, receiver: String, time: String)
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}

