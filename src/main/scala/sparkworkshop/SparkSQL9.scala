import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil, Verse}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Example of SparkSQL, both SQL queries and the new DataFrame API,
 * using the KJV Bible text.
 */
object SparkSQL9 {

  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-spark-sql"),
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
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

    val name = "Spark SQL (9)"
    val conf = new SparkConf().
      setMaster(master).
      setAppName(name).
      set("spark.app.id", name)   // To silence Metrics warning.
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    import sqlContext.sql    // Convenient for running SQL queries.

    try {
      // Regex to match the fields separated by "|".
      // Also strips the trailing "~" in the KJV file.
      val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
      // Use flatMap to effectively remove bad lines.
      val versesRDD = sc.textFile(argz("input-path")) flatMap {
        case lineRE(book, chapter, verse, text) =>
          Seq(Verse(book, chapter.toInt, verse.toInt, text))
        case line =>
          Console.err.println(s"Unexpected line: $line")
          Seq.empty[Verse]  // Will be eliminated by flattening.
      }

      // Create a DataFrame and register as a temporary "table".
      // The following expression invokes several "implicit" conversions and
      // methods that we imported through sqlContext._ The actual method is
      // defined on org.apache.spark.sql.SchemaRDDLike, which also has a method
      // "saveAsParquetFile" to write a schema-preserving Parquet file.
      val verses = sqlContext.createDataFrame(versesRDD)
      verses.registerTempTable("kjv_bible")
      verses.cache()
      // print the 1st 20 lines (default: pass another integer as the argument
      // to show() for a different number of lines).
      if (!quiet) {
        verses.show()
      }

      val godVerses = sql("SELECT * FROM kjv_bible WHERE text LIKE '%God%'")
      if (!quiet) {
        println("The query plan:")
        // Compare with println(godVerses.queryExecution)
        godVerses.explain(true)
        println("Number of verses that mention God: "+godVerses.count())
        godVerses.show()
      }

      // Use the DataFrame API:
      val godVersesDF = verses.filter(verses("text").contains("God"))
      if (!quiet) {
        println("The query plan:")
        godVersesDF.explain(true)
        println("Number of verses that mention God: "+godVersesDF.count())
        godVersesDF.show()
      }
      godVersesDF.rdd.saveAsTextFile(outgv)

      // NOTE: The basic SQL dialect currently supported doesn't permit
      // column aliasing, e.g., "COUNT(*) AS count". This makes it difficult
      // to write the following query result to Parquet, for example.
      // Nor does it appear to support WHERE clauses in some situations.
      val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
      if (!quiet) {
        counts.show(100)  // print all the book counts
      }

      // Use "coalesce" when you have too many small partitions. The integer
      // passed to "coalesce" is the number of output partitions (1 in this case).
      val counts1 = counts.coalesce(1)
      if (!quiet) {
        val nPartitions  = counts.rdd.partitions.size
        val nPartitions1 = counts1.rdd.partitions.size
        println(s"counts.count (can take a while, #$nPartitions partitions):")
        println(s"result: ${counts.count}")
        println(s"counts1.count (usually faster!! #$nPartitions1 partitions):")
        println(s"result: ${counts1.count}")
      }
      counts1.rdd.saveAsTextFile(outvpb)

    } finally {
      sc.stop()
    }

    // For the following exercises, when you're running in local mode, consider
    // using the "show" method to write the output to the console instead of
    // to a file.
    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
    //   abbreviations to full titles. (See the provides solution as a script,
    //   solns/SparkSQL-..-script.scala)
    // Exercise: Play with the DataFrame DSL.
    // Exercise: Try some of the other sacred text data files.
  }
}
