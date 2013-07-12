import com.twitter.sbt._

seq((
  Project.defaultSettings ++
    StandardProject.newSettings ++
    SubversionPublisher.newSettings ++
    CompileThriftScrooge.newSettings
): _*)

organization := "com.twitter"

name := "serverset"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.8.1"

libraryDependencies ++= Seq(
  // some hand-wringing here to avoid pulling in a pile of ridiculous libraries that finagle-core wants, but doesn't actually need.
  "com.twitter" % "util-zk" % "4.0.0" intransitive(),
  "org.apache.thrift" % "libthrift" % "0.8.0",
  "com.twitter" % "util-core" % "4.0.1" intransitive(),
  "com.twitter" % "util-codec" % "4.0.1" intransitive(),
  "com.twitter" % "util-logging" % "4.0.0" intransitive(),
  "com.codahale" % "jerkson_2.8.2" % "0.5.0",
  "org.apache.zookeeper" % "zookeeper" % "3.3.4",
  "com.twitter" %% "scrooge-runtime" % "3.0.0-SNAPSHOT",
  "org.slf4j" % "slf4j-nop" % "1.6.4", // bug in apache-zookeeper
  // tests:
  "org.scalatest" %% "scalatest" % "1.7.1" % "test",
  "org.mockito" % "mockito-core" % "1.9.0" % "test"
)

mainClass in (Compile, run) := Some("com.twitter.serverset.Main")

CompileThriftScrooge.scroogeVersion := "3.0.0-SNAPSHOT"

CompileThriftScrooge.scroogeBuildOptions := Seq("--verbose")

