name := "sbt-spark"

version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies += "org.scala-lang" % "scala-library" % scalaVersion.value
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"