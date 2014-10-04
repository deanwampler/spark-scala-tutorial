initialCommands += """
  |import org.apache.spark.SparkContext
  |import org.apache.spark.SparkContext._
  |val sc = new SparkContext("local", "Intro")
  |""".stripMargin

cleanupCommands += """
  |println("Closing the SparkContext:")
  |sc.stop()
  |""".stripMargin

addCommandAlias("ex2",  "runMain WordCount2")

addCommandAlias("ex3",  "runMain WordCount3")

addCommandAlias("ex4",  "runMain Matrix4")

addCommandAlias("ex5a", "runMain Crawl5a")

addCommandAlias("ex5b", "runMain InvertedIndex5b")

addCommandAlias("ex6",  "runMain NGrams6")

addCommandAlias("ex7",  "runMain Joins7")

addCommandAlias("ex8",  "runMain SparkStreaming8")

addCommandAlias("ex9",  "runMain SparkSQL9")

// Command aliases for the Hadoop drivers.
// Note: there is no Hadoop version for WordCount2.

addCommandAlias("hex3",  "runMain hadoop.HWordCount3")

addCommandAlias("hex4",  "runMain hadoop.HMatrix4")

addCommandAlias("hex5a", "runMain hadoop.HCrawl5a")

addCommandAlias("hex5b", "runMain hadoop.HInvertedIndex5b")

addCommandAlias("hex6",  "runMain hadoop.HNGrams6")

addCommandAlias("hex7",  "runMain hadoop.HJoins7")

addCommandAlias("hex8",  "runMain hadoop.HSparkStreaming8")

addCommandAlias("hex9",  "runMain hadoop.HSparkSQL9")
