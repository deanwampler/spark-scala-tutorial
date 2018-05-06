name          := "spark-scala-tutorial"
organization  := "com.lightbend"
description   := "Spark Spark Tutorial"
version       := "6.0.0"
scalaVersion  := "2.11.8"
scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
excludeFilter in unmanagedSources := (HiddenFileFilter || "*-script.scala")
unmanagedResourceDirectories in Compile += baseDirectory.value / "conf"
unmanagedResourceDirectories in Test += baseDirectory.value / "conf"
//This is important for some programs to read input from stdin
connectInput in run := true
// Works better to run the examples and tests in separate JVMs.
fork := true
// Must run Spark tests sequentially because they compete for port 4040!
parallelExecution in Test := false

val sparkVersion        = "2.3.0"
val scalaTestVersion    = "3.0.5"
val scalaCheckVersion   = "1.13.4"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"      % sparkVersion,
  "org.apache.spark"  %% "spark-streaming" % sparkVersion,
  "org.apache.spark"  %% "spark-sql"       % sparkVersion,
  "org.apache.spark"  %% "spark-hive"      % sparkVersion,
  "org.apache.spark"  %% "spark-repl"      % sparkVersion,

  "org.scalatest"     %% "scalatest"       % scalaTestVersion  % "test",
  "org.scalacheck"    %% "scalacheck"      % scalaCheckVersion % "test")


initialCommands += """
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.SparkContext
  val spark = SparkSession.builder.
    master("local[*]").
    appName("Console").
    config("spark.app.id", "Console").   // To silence Metrics warning.
    getOrCreate()
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  """

cleanupCommands += """
  println("Closing the SparkSession:")
  spark.stop()
  """

addCommandAlias("ex2",  "runMain WordCount2")
addCommandAlias("ex3",  "runMain WordCount3")
addCommandAlias("ex4",  "runMain Matrix4")
addCommandAlias("ex5a", "runMain Crawl5a")
addCommandAlias("ex5b", "runMain InvertedIndex5b")
addCommandAlias("ex6",  "runMain NGrams6")
addCommandAlias("ex7",  "runMain Joins7")
addCommandAlias("ex8",  "runMain SparkSQL8")

// Note the differences in the next two definitions:
addCommandAlias("ex10directory",  "runMain SparkStreaming10Main")
addCommandAlias("ex10socket",     "runMain SparkStreaming10Main --socket localhost:9900")
