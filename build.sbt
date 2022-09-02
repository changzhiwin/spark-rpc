name := "spark-rpc"
organization := "xyz.sourcecodestudy.rpc" // change to your org
version := "1.0.0"

scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
  // "org.scalacheck" %% "scalacheck" % "1.15.4" % "test",

  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.17.2",

  // https://mvnrepository.com/artifact/com.google.guava/guava/31.1-jre
  "com.google.guava" % "guava" % "31.1-jre",
  // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/ClosureCleaner.scala
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  //"org.apache.xbean" % "xbean-asm9-shaded" % "4.21",
  "commons-io" % "commons-io" % "2.11.0",

  // java impliment for netty framework
  "org.apache.spark" %% "spark-network-common" % "3.3.0",

  // config
  "com.typesafe" % "config" % "1.4.2"
)  

Compile / mainClass := Some("xyz.sourcecodestudy.rpc.demo.Foobar")

Compile / scalacOptions ++= List("-feature", "-deprecation", "-unchecked", "-Xlint")

Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "org.scalatest.tags.Slow", "-u", "target/junit-xml-reports", "-oD", "-eS")
