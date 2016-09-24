import sbt._
import sbt.Keys._

object BuildSettings {

  val Name = "spark-scala-tutorial"
  val Version = "5.0.0"
  // You can use either version of Scala. We default to 2.11.8:
  val ScalaVersion = "2.11.8"
  val ScalaVersions = Seq("2.11.8", "2.10.6")

  lazy val buildSettings = Defaults.coreDefaultSettings ++ Seq (
    name          := Name,
    version       := Version,
    scalaVersion  := ScalaVersion,
    crossScalaVersions := ScalaVersions,
    organization  := "com.lightbend",
    description   := "Spark Spark Tutorial",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8", "-Xlint")
  )
}


object Resolvers {
  val lightbend = "Lightbend Repository" at "http://repo.lightbend.com/lightbend/releases/"
  val sonatype = "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
  val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"

  val allResolvers = Seq(lightbend, sonatype, mvnrepository)

}

// We don't actually use all these dependencies, but they are shown for the
// examples that explicitly use Hadoop.
object Dependency {
  object Version {
    val Spark        = "1.6.2"
    val ScalaTest    = "2.2.4"
    val ScalaCheck   = "1.12.2"
  }

  val sparkCore      = "org.apache.spark"  %% "spark-core"      % Version.Spark
  val sparkStreaming = "org.apache.spark"  %% "spark-streaming" % Version.Spark
  val sparkSQL       = "org.apache.spark"  %% "spark-sql"       % Version.Spark
  val sparkHiveSQL   = "org.apache.spark"  %% "spark-hive"      % Version.Spark
  val sparkRepl      = "org.apache.spark"  %% "spark-repl"      % Version.Spark

  val scalaTest      = "org.scalatest"     %% "scalatest"       % Version.ScalaTest  % "test"
  val scalaCheck     = "org.scalacheck"    %% "scalacheck"      % Version.ScalaCheck % "test"
}

object Dependencies {
  import Dependency._

  val sparkdeps =
    Seq(sparkCore, sparkStreaming, sparkSQL, sparkHiveSQL, // sparkRepl,
      scalaTest, scalaCheck)
}

object SparkBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  val excludeSigFilesRE = """META-INF/.*\.(SF|DSA|RSA)""".r
  lazy val spark_scala_tutorial = Project(
    id = "SparkWorkshop",
    base = file("."),
    settings = buildSettings ++ Seq(
      maxErrors          := 5,
      triggeredMessage   := Watched.clearWhenTriggered,
      // runScriptSetting,
      resolvers := allResolvers,
      exportJars := true,
      // For the Hadoop variants to work, we must rebuild the package before
      // running, so we make it a dependency of run.
      (run in Compile) <<= (run in Compile) dependsOn (packageBin in Compile),
      libraryDependencies ++= Dependencies.sparkdeps,
      excludeFilter in unmanagedSources := (HiddenFileFilter || "*-script.scala"),
      unmanagedResourceDirectories in Compile += baseDirectory.value / "conf",
      unmanagedResourceDirectories in Test += baseDirectory.value / "conf",
      mainClass := Some("run"),
      //This is important for some programs to read input from stdin
      connectInput in run := true,
      // Works better to run the examples and tests in separate JVMs.
      fork := true,
      // Must run Spark tests sequentially because they compete for port 4040!
      parallelExecution in Test := false))
}



