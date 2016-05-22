name := "bloomfilter"

version := "0.2.0-SNAPSHOT"

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.6", "2.11.8", "2.12.0-M4")

scalacOptions := Seq("-deprecation")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "log4j" % "log4j" % "1.2.17" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.21" % "test",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21" % "test"
)
