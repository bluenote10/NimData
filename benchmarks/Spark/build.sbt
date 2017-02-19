lazy val root = (project in file("."))

name := "SparkBenchmark"
version := "1.0"
scalaVersion := "2.11.8"
mainClass in (Compile, run) := Some("nodomain.Benchmark")

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"

fork in run := true
outputStrategy := Some(StdoutOutput)
javaOptions in run += "-DXmx8G"

