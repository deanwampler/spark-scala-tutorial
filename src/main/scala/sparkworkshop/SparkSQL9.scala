import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil, Verse}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

/**
 * Example of SparkSQL, using the KJV Bible text.
 */
object SparkSQL9 {

  // Trying using this method to dump an RDD to the console when running locally.
  // By default, it prints the first 100 lines of output, but you can call dump
  // with another number as the second argument to change that.
  def dump(rdd: RDD[_], n: Int = 100) = rdd
      .collect()        // Convert to a regular in-memory collection.
      .take(n)          // Take the first n lines.
      .foreach(println) // Print the query results.

  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-spark-sql"),
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master").toString
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path").toString
    val outgv  = s"$out-god-verses"
    val outvpb = s"$out-verses-per-book"
    if (master.startsWith("local")) {
      if (!quiet) {
        println(s" **** Deleting old output (if any), $outgv:")
        println(s" **** Deleting old output (if any), $outvpb:")
      }
      FileUtil.rmrf(outgv)
      FileUtil.rmrf(outvpb)
    }

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

      if (!quiet) println(s"Writing verses that mention God to: $outgv")
      godVerses.saveAsTextFile(outgv)

      // NOTE: The basic SQL dialect currently supported doesn't permit
      // column aliasing, e.g., "COUNT(*) AS count". This makes it difficult
      // to write the following query result to Parquet, for example.
      // Nor does it appear to support WHERE clauses in some situations.
      val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
        // Collect all partitions into 1 partition. Otherwise, there are 100s
        // output from the last query!
        .coalesce(1)

      if (!quiet) println(s"Writing count of verses per book to: $outvpb")
      counts.saveAsTextFile(outvpb)

    } finally {
      sc.stop()
    }

    // For the following exercises, when you're running in local mode, consider
    // using the "dump" method defined above to dump the output to the console
    // instead of writing to a file.
    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
    //   abbreviations to full titles.
    // Exercise: Play with the SchemaRDD DSL.
    // Exercise: Try some of the other sacred text data files.
  }
}
