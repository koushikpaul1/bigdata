package com.edge.old.streaming.TCP.unstructured

import java.util.regex.Matcher

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.edge.old.streaming.Utilities._
/** Maintains top URL's visited over a 5 minute window, from a stream  *  of Apache access logs on port 9999.  *
  * start ncat ot port 9999 feeding from a log file/folder =>ncat -kl 9999 < access_log.txt
 */
object AClickStreamLogParser {

  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))
    Logger.getLogger("org").setLevel(Level.ERROR)
    val pattern = apacheLogPattern()
    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    ssc.checkpoint("D:/temp/spark/twitter/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
