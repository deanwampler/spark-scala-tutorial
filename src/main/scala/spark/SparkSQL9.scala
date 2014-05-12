package spark

import spark.util.{Timestamp, CommandLineOptions, Verse}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

/** 
 * Example of Spark SQL, using the KJV Bible text. 
 * Writes "query" results to the console, rather than a file.
 */
object SparkSQL9 {

  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.master("local"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Spark SQL (9)")
    val sqlContext = new SQLContext(sc)
    import sqlContext._    // Make its methods accessible.

    try {
      // Regex to match the fields separated by "|". 
      // Also strips the trailing "~" in the KJV file.
      val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
      // Use flatMap to effectively remove bad lines.
      val verses = sc.textFile(argz("input-path").toString) flatMap {
        case lineRE(book, chapter, verse, text) => 
          List(Verse(book, chapter.toInt, verse.toInt, text))
        case line => 
          Console.err.println("Unexpected line: $line")
          Nil  // Will be eliminated by flattening.
      }

      // Register the RDD as a temporary table in the Hive metadata store.
      // The following expression invokes several "implicit" conversions and 
      // methods that we imported through sqlContext._ The actual method is
      // defined on org.apache.spark.sql.SchemaRDDLike, which also has a method
      // "saveAsParquetFile" to write a schema-preserving Parquet file.
      verses.registerAsTable("bible")
      verses.cache()

      val godVerses = sql("SELECT * FROM bible WHERE text LIKE '%God%';")    
      println("Number of verses that mention God: "+godVerses.count())
      godVerses
        .collect()   // convert to a regular in-memory collection
        .foreach(println) // print the query results.

      val counts = sql("""
        |SELECT * FROM (
        |  SELECT book, COUNT(*) FROM bible GROUP BY book) bc
//        |  SELECT book, COUNT(*) AS count FROM bible GROUP BY book) bc
        |WHERE bc.book <> '';
        |""".stripMargin)

      println("Verses per book:")
      counts
        // Collect all partitions into 1 partition. Otherwise, there are 100s
        // output from the last query!
        .coalesce(1)
        .collect()        // Convert to a regular in-memory collection.
        .foreach(println) // Print the query results.

      // Let's also see how to use the API to read and write Parquet files:
      counts.saveAsParquetFile("output/sql.parquet")
      
      // Now read it back and use it as a table:
      val counts2 = sqlContext.parquetFile("output/sql.parquet")
      counts2.registerAsTable("countS2")
      // Run a SQL query against the table:
      println("Using the Parquet File, order the books-counts by count descending:")
      sql("SELECT c.book, c.count FROM counts2 c ORDER BY c.count DESC;")
        .collect().foreach(println)

      // ... or use the LINQ-inspired DSL that's provided by the class 
      // org.apache.spark.sql.SchemaRDD that's used implicitly:
      println("Repeat the ORDER BY using the LINQ-style query API:")
      counts2.orderBy('count.desc).select('book, 'count)
        .collect().foreach(println)

    } finally {
      sc.stop()
    }

    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book 
    //   abbreviations to full titles.
    // Exercise: Play with the SchemaRDD DSL.
    // Exercise: Try some of the other sacred text data files.
  }
}
