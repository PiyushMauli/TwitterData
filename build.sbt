name := "Twitter_Live"

version := "0.1"

scalaVersion := "2.11.8"

import AssemblyPlugin._

assemblySettings

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.2"

// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-core
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

// https://mvnrepository.com/artifact/org.twitter4j/twitter4j-stream
libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.6.0"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"

// https://mvnrepository.com/artifact/javax.servlet/servlet-api
libraryDependencies += "javax.servlet" % "servlet-api" % "2.5"

// https://mvnrepository.com/artifact/net.java.dev.jets3t/jets3t
libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.9.4"

// https://mvnrepository.com/artifact/org.apache.httpcomponents/httpclient
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.6"

resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

resolvers += Resolver.bintrayIvyRepo("com.eed3si9n", "sbt-plugins")

lazy val commonSettings = Seq(
  organization := "com.bridgelabz",
  version := "0.1.0-SNAPSHOT"
)

// set the main class for packaging the main jar
mainClass in (Compile, packageBin) := Some("TweetTwitter")

// set the main class for the main 'sbt run' task
mainClass in (Compile, run) := Some("TweetTwitter")


resolvers in Global ++= Seq(
  "Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "TypeSafe Repository Releases" at "http://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "http://repo.typesafe.com/typesafe/snapshots/"
)

lazy val root = (project in file(".")).
  settings(
    name := "Week10V2.0",
    version := "0.1",
    scalaVersion := "2.12.8",
    Compile / mainClass := Some("TweetTwitter")
  )
  .settings(commonSettings: _*)
  .enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

