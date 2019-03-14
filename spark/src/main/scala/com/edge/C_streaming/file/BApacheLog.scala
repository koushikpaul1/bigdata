package com.edge.C_streaming.file

//This is a streaming app pointing to a log dir, as more and more logs get appended in the dir, it automatically picks up those.
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}
import com.edge.C_streaming.TCP.Utilities._
import org.apache.log4j._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.{Row, SparkSession}

object BApacheLog {
  case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)
   val logPattern = apacheLogPattern()
   val datePattern = Pattern.compile("\\[(.*?) .+]")

     def parseDateField(field: String): Option[String] = {
      val dateMatcher = datePattern.matcher(field)
      if (dateMatcher.find) {
              val dateString = dateMatcher.group(1)
              val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
              val date = (dateFormat.parse(dateString))
              val timestamp = new java.sql.Timestamp(date.getTime());
              return Option(timestamp.toString())
          } else {
          None
      }
   }

    def parseLog(x:Row) : Option[LogEntry] = {
     val matcher:Matcher = logPattern.matcher(x.getString(0));
     if (matcher.matches()) {
       val timeString = matcher.group(4)
       return Some(LogEntry(
           matcher.group(1),
           matcher.group(2),
           matcher.group(3),
           parseDateField(matcher.group(4)).getOrElse(""),
           matcher.group(5),
           matcher.group(6),
           matcher.group(7),
           matcher.group(8),
           matcher.group(9)
           ))
     } else {
       return None
     }
   }
     def main(args: Array[String]) {
      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///D:/temp/spark/twitter/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .config("spark.sql.streaming.checkpointLocation", "file:///D:/temp/spark/twitter/checkpoint")
        .getOrCreate()
       Logger.getLogger("org").setLevel(Level.ERROR)
      val rawData = spark.readStream.text("input/udemy/streaming/logs")
      // Must import spark.implicits for conversion to DataSet to work!
      import spark.implicits._
      val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")
      val windowed = structuredData.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
      val query = windowed.writeStream.outputMode("complete").format("console").start()
      query.awaitTermination()
      spark.stop()
   }

}
