initialCommands += """
  |import org.apache.spark.SparkContext
  |import org.apache.spark.SparkContext._
  |val sc = new SparkContext("local", "Intro")
  |""".stripMargin

cleanupCommands += """
  |println("Closing the SparkContext:")
  |sc.stop()
  |""".stripMargin

addCommandAlias("ex2", "runMain com.typesafe.sparkworkshop.WordCount2")

addCommandAlias("ex3",  "runMain com.typesafe.sparkworkshop.WordCount3")

addCommandAlias("ex4",  "runMain com.typesafe.sparkworkshop.Matrix4")

addCommandAlias("ex5a", "runMain com.typesafe.sparkworkshop.Crawl5a")

addCommandAlias("ex5b", "runMain com.typesafe.sparkworkshop.InvertedIndex5b")

addCommandAlias("ex6",  "runMain com.typesafe.sparkworkshop.NGrams6")

addCommandAlias("ex7",  "runMain com.typesafe.sparkworkshop.Joins7")

addCommandAlias("ex8",  "runMain com.typesafe.sparkworkshop.SparkStreaming8")

addCommandAlias("ex9",  "runMain com.typesafe.sparkworkshop.SparkSQL9")
