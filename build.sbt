name := "kafka-consumer-actor"

version := "1.0"

scalaVersion := "2.11.12"

lazy val kafkaVersion = "0.10.0.0"
lazy val akkaVersion = "2.5.13"

libraryDependencies ++= Seq(
  //Kafka and Kafka-Unit dependencies
  "org.apache.kafka" %% "kafka" % kafkaVersion,
  "info.batey.kafka" % "kafka-unit" % "0.6",

  //Akka
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)