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

// Command aliases for the Hadoop drivers:

addCommandAlias("hex2",  "runMain HWordCount2")

addCommandAlias("hex3",  "runMain HWordCount3")

addCommandAlias("hex4",  "runMain HMatrix4")

addCommandAlias("hex5a", "runMain HCrawl5a")

addCommandAlias("hex5b", "runMain HInvertedIndex5b")

addCommandAlias("hex6",  "runMain HNGrams6")

addCommandAlias("hex7",  "runMain HJoins7")

addCommandAlias("hex8",  "runMain HSparkStreaming8")

addCommandAlias("hex9",  "runMain HSparkSQL9")
