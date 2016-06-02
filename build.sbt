import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import sbt._
import Keys._

assemblySettings

name := "druid_starter"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "log4j"              % "log4j"            % "1.2.17",
  "org.slf4j"          % "slf4j-log4j12"    % "1.7.2",
  "org.scalatest"     %% "scalatest"   % "2.2.2",
  "org.apache.spark"  %% "spark-core"   % "1.3.0",
  "org.apache.spark"  %% "spark-sql"   % "1.3.0",
  "org.apache.spark"  %% "spark-hive"   % "1.3.0",
  "org.apache.spark"  %% "spark-streaming"   % "1.3.0",
  "io.druid"          %% "tranquility-core" % "0.8.0",
  "io.druid"          %% "tranquility-spark" % "0.8.0"

)

mergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".SF" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".DSA" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".RSA" => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "MANIFEST.MF" => MergeStrategy.discard
  case _  => MergeStrategy.first

}

