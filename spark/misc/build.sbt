name := "learningSpark"
version := "0.1"
scalaVersion := "2.11.8"
val sparkVersion="2.4.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "runtime",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-repl" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-yarn" % sparkVersion,
  "org.apache.spark" %% "spark-mllib-local" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"
)
