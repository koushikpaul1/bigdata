package com.edge.old.streaming.TCP.structured

import java.util.regex.Matcher

import org.apache.log4j._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.edge.old.streaming.Utilities._
object  ALogSQL {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("LogSQL")
      .setMaster("local[*]")
      .set("spark.sql.warehouse.dir", "file:///D:/tmp")
    val ssc = new StreamingContext(conf, Seconds(1))
    Logger.getLogger("org").setLevel(Level.ERROR)
    val pattern = apacheLogPattern()
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })

    requests.foreachRDD((rdd, time) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3)).toDF()// From Unstructured >map to case clas> toDF/toDS
      requestsDataFrame.createOrReplaceTempView("requests")
      val wordCountsDataFrame =
        sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"========= $time =========")
      wordCountsDataFrame.show()
    })
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
  case class Record(url: String, status: Int, agent: String)
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
