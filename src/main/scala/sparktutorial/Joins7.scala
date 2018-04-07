import util.{CommandLineOptions, FileUtil}
import util.CommandLineOptions.Opt
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
 * Joins7 - Perform joins of datasets.
 *
 * @deprecated - Use the SparkSQL join implementations instead. They are far more performant!
 */
object Joins7 {
  def main(args: Array[String]): Unit = {

    /** The "dictionary" of book abbreviations to full names */
    val abbrevsFile = "data/abbrevs-to-names.tsv"
    val abbrevs = Opt(
      name   = "abbreviations",
      value  = abbrevsFile,
      help   = s"-a | --abbreviations  path The dictionary of book abbreviations to full names (default: $abbrevsFile)",
      parser = {
        case ("-a" | "--abbreviations") +: path +: tail => (("abbreviations", path), tail)
      })

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      abbrevs,
      CommandLineOptions.outputPath("output/kjv-joins"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    val name = "Joins (7)"
    val spark = SparkSession.builder.
      master("local[*]").
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      getOrCreate()
    val sc = spark.sparkContext

    try {
      // Load one of the religious texts, don't convert each line to lower case
      // this time, then extract the fields in the "book|chapter|verse|text" format
      // used for each line, creating an RDD. However, note that the logic used
      // to split the line will work reliably even if the delimiters aren't present!
      // Note also the output nested tuple. Joins only work for RDDs of
      // (key,value) tuples
      val input = sc.textFile(argz("input-path"))
        .map { line =>
          val ary = line.split("\\s*\\|\\s*")
          (ary(0), (ary(1), ary(2), ary(3)))
        }

      // The abbreviations file is tab separated, but we only want to split
      // on the first space (in the unlikely case there are embedded tabs
      // in the names!)
      val abbrevs = sc.textFile(argz("abbreviations"))
        .map{ line =>
          val ary = line.split("\\s+", 2)
          (ary(0), ary(1).trim)  // I've noticed trailing whitespace...
        }

      // Cache both RDDs in memory for fast, repeated access.
      input.cache
      abbrevs.cache

      // Join on the key, the first field in the tuples; the book abbreviation.

      val verses = input.join(abbrevs)

      if (input.count != verses.count) {
        println(s"input count, ${input.count}, doesn't match output count, ${verses.count}")
      }

      // Project out the flattened data we want:
      //   fullBookName|chapter|verse|text

      val verses2 = verses.map {
        // Drop the key - the abbreviated book name
        case (_, ((chapter, verse, text), fullBookName)) =>
          (fullBookName, chapter, verse, text)
      }

      if (!quiet) println(s"Writing output to: $out")
      verses2.saveAsTextFile(out)
    } finally {
      spark.stop()
    }

    // Exercise: Try different sacred text files.
    // Exercise: Try outer joins (see http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions).
    // Exercise (hard): The output does NOT preserve the original order of the
    //   verses! This is a consequence of how joins are implemented ("co-groups").
    //   Fix the ordering. Here is one approach:
    //   Compute (in advance??) a map from book names (or abbreviations) to
    //   an index (e.g., Gen -> 1, Exo -> 2, ...). Use this to construct a
    //   sort key containing the book index, chapter, and verse. Note that
    //   the chapter and verse will be strings when extracted from the file,
    //   so you must convert them to integers (i.e., "x.toInt"). Finally,
    //   project out the full book name, chapter, verse, and text.
  }
}
