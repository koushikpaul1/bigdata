package com.edge.C_streaming.TCP.unstructured

import java.util.regex.Matcher
import com.edge.C_streaming.TCP.Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.edge.C_streaming.TCP.Utilities._

/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
 *  if an excessive ratio of errors is encountered .error/success/ >1/2 or 50%
 */
object BClickStreamLogAlarmer {

  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))
    setupLogging()
    val pattern = apacheLogPattern()
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val statuses = lines.map(x => {val matcher:Matcher = pattern.matcher(x);if (matcher.matches()) matcher.group(6) else "[error]"})

    // Now map these status results to success and failure
    val successFailure = statuses.map(x => {val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300) { "Success"
      } else if (statusCode >= 500 && statusCode < 600) {"Failure"
      } else {"Other"}
    })
    val statusCounts = successFailure.countByValueAndWindow(Seconds(300), Seconds(1))
    statusCounts.foreachRDD((rdd, time) => {
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect()
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {totalSuccess += count}
          if (result == "Failure") {totalError += count}
        }
      }
      println("Total success: " + totalSuccess + " Total failure: " + totalError)
      if (totalError + totalSuccess > 100) {
        val ratio:Double = util.Try( totalError.toDouble / totalSuccess.toDouble ) getOrElse 1.0
        if (ratio > 0.5) {
          println("Wake somebody up! Something is horribly wrong.")
        } else {
          println("All systems go.")
        }
      }
    })
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
