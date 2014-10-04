import com.typesafe.sparkworkshop.util.{CommandLineOptions, Verse}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

/**
 * Example of SparkSQL, using the KJV Bible text.
 * Writes "query" results to the console, rather than a file.
 */
object SparkSQL9 {

  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.quiet)

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Spark SQL (9)")
    val sqlc = new SQLContext(sc)
    import sqlc._

    try {
      // Regex to match the fields separated by "|".
      // Also strips the trailing "~" in the KJV file.
      val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
      // Use flatMap to effectively remove bad lines.
      val verses = sc.textFile(argz("input-path").toString) flatMap {
        case lineRE(book, chapter, verse, text) =>
          Seq(Verse(book, chapter.toInt, verse.toInt, text))
        case line =>
          Console.err.println("Unexpected line: $line")
          Seq.empty[Verse]  // Will be eliminated by flattening.
      }

      // Register the RDD as a temporary "table".
      // The following expression invokes several "implicit" conversions and
      // methods that we imported through sqlc._ The actual method is
      // defined on org.apache.spark.sql.SchemaRDDLike, which also has a method
      // "saveAsParquetFile" to write a schema-preserving Parquet file.
      verses.registerTempTable("kjv_bible")
      verses.cache()

      val godVerses = sql("SELECT * FROM kjv_bible WHERE text LIKE '%God%'")
      println("Number of verses that mention God: "+godVerses.count())
      godVerses
        .collect()   // convert to a regular in-memory collection
        .foreach(println) // print the query results.

      // NOTE: The basic SQL dialect currently supported doesn't permit
      // column aliasing, e.g., "COUNT(*) AS count". This makes it difficult
      // to write the following query result to Parquet, for example.
      // Nor does it appear to support WHERE clauses in some situations.
      val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
        // Collect all partitions into 1 partition. Otherwise, there are 100s
        // output from the last query!
        .coalesce(1)

      println("Verses per book:")
      counts
        .collect()        // Convert to a regular in-memory collection.
        .foreach(println) // Print the query results.

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
