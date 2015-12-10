initialCommands += """
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.SparkContext._
  import org.apache.spark.sql.SQLContext
  val conf = new SparkConf().
    setMaster("local[*]").
    setAppName("Console").
    set("spark.app.id", "Console")   // To silence Metrics warning.
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._    // for min, max, etc.
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """

addCommandAlias("ex2",  "run-main WordCount2")
addCommandAlias("ex3",  "run-main WordCount3")
addCommandAlias("ex4",  "run-main Matrix4")
addCommandAlias("ex5a", "run-main Crawl5a")
addCommandAlias("ex5b", "run-main InvertedIndex5b")
addCommandAlias("ex6",  "run-main NGrams6")
addCommandAlias("ex7",  "run-main Joins7")
addCommandAlias("ex8",  "run-main SparkSQL8")

// Note the differences in the next two definitions:
addCommandAlias("ex11directory",  "run-main SparkStreaming11Main")
addCommandAlias("ex11socket",     "run-main SparkStreaming11Main --socket localhost:9900")

// Command aliases for the Hadoop drivers.
// Note: there is no Hadoop version for WordCount2.
addCommandAlias("hex3",  "run-main hadoop.HWordCount3")
addCommandAlias("hex4",  "run-main hadoop.HMatrix4")
addCommandAlias("hex5a", "run-main hadoop.HCrawl5aHDFS")
addCommandAlias("hex5b", "run-main hadoop.HInvertedIndex5b")
addCommandAlias("hex6",  "run-main hadoop.HNGrams6")
addCommandAlias("hex7",  "run-main hadoop.HJoins7")
addCommandAlias("hex8",  "run-main hadoop.HSparkSQL8")
addCommandAlias("hex11", "run-main hadoop.HSparkStreaming11")

fork in run := true

