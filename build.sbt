name := "nycbikestationstream"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.6.2",
  "org.apache.spark" % "spark-streaming_2.11" % "1.6.2",
  "org.apache.spark" % "spark-sql_2.11" % "1.6.2",
  "com.typesafe.play" % "play-json_2.11" % "2.4.0-M2"
)