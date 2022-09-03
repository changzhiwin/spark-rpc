name := "spark-rpc"
organization := "xyz.sourcecodestudy.rpc" // change to your org
version := "1.0.0"

scalaVersion := "2.13.8"

val AkkaVersion = "2.6.19"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
  // "org.scalacheck" %% "scalacheck" % "1.15.4" % "test",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2",

  // https://mvnrepository.com/artifact/com.google.guava/guava/31.1-jre
  "com.google.guava" % "guava" % "31.1-jre",

  // netty for network
  "org.apache.spark" %% "spark-network-common" % "3.3.0",

  // config
  "com.typesafe" % "config" % "1.4.2",

  // akka
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
)  

Compile / mainClass := Some("xyz.sourcecodestudy.rpc.demo.Foobar")

Compile / scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")
