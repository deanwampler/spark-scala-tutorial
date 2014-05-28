import sbt._
import sbt.Keys._

object BuildSettings {

  val Name = "activator-spark"
  val Version = "1.0.0"
  val ScalaVersion = "2.10.4"

  lazy val buildSettings = Defaults.defaultSettings ++ Seq (
    name          := Name,
    version       := Version,
    scalaVersion  := ScalaVersion,
    organization  := "com.typesafe",
    description   := "Activator Spark Template",
    scalacOptions := Seq("-deprecation", "-unchecked", "-encoding", "utf8")
  )
}


object Resolvers {
  // This is a temporary location within the Apache repo for the 1.0.0-RC3
  // release of Spark.
  val apache = "Apache Repository" at "https://repository.apache.org/content/repositories/orgapachespark-1012/"
  val typesafe = "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
  val sonatype = "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases"
  val mvnrepository = "MVN Repo" at "http://mvnrepository.com/artifact"

  val allResolvers = Seq(apache, typesafe, sonatype, mvnrepository)

}

object Dependency {
  object Version {
    val Spark      = "0.9.1" //"1.0.0"
    val ScalaTest  = "2.1.4"
    val ScalaCheck = "1.11.3"
  }

  val sparkCore      = "org.apache.spark" %% "spark-core"      % Version.Spark
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % Version.Spark   
  val sparkRepl      = "org.apache.spark" %% "spark-repl"      % Version.Spark   

  val scalaTest      = "org.scalatest"    %% "scalatest"       % Version.ScalaTest  % "test"
  val scalaCheck     = "org.scalacheck"   %% "scalacheck"      % Version.ScalaCheck % "test"
}

object Dependencies {
  import Dependency._

  val activatorspark = 
    Seq(sparkCore, sparkStreaming, sparkRepl, scalaTest, scalaCheck)
}

object ActivatorSparkBuild extends Build {
  import Resolvers._
  import Dependencies._
  import BuildSettings._

  lazy val activatorspark = Project(
    id = "Activator-Spark",
    base = file("."),
    settings = buildSettings ++ Seq(
      // runScriptSetting, 
      resolvers := allResolvers,
      libraryDependencies ++= Dependencies.activatorspark,
      unmanagedResourceDirectories in Compile += baseDirectory.value / "conf",
      mainClass := Some("run"),
      // Must run Spark jobs sequentially because they compete for port 4040!
      parallelExecution in Test := false))
}



