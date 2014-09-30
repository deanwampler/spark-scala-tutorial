initialCommands += """
  |import org.apache.spark.SparkContext
  |import org.apache.spark.SparkContext._
  |val sc = new SparkContext("local", "Intro")
  |""".stripMargin

cleanupCommands += """
  |println("Closing the SparkContext:")
  |sc.stop()
  |""".stripMargin

addCommandAlias("ex2", "runMain com.typesafe.activator.spark.WordCount2")

addCommandAlias("ex3",  "runMain com.typesafe.activator.spark.WordCount3")

addCommandAlias("ex4",  "runMain com.typesafe.activator.spark.Matrix4")

addCommandAlias("ex5a", "runMain com.typesafe.activator.spark.Crawl5a")

addCommandAlias("ex5b", "runMain com.typesafe.activator.spark.InvertedIndex5b")

addCommandAlias("ex6",  "runMain com.typesafe.activator.spark.NGrams6")

addCommandAlias("ex7",  "runMain com.typesafe.activator.spark.Joins7")

addCommandAlias("ex8",  "runMain com.typesafe.activator.spark.SparkStreaming8")

addCommandAlias("ex9",  "runMain com.typesafe.activator.spark.SparkSQL9")

addCommandAlias("ex10", "runMain com.typesafe.activator.spark.SparkSQLParquet10")
