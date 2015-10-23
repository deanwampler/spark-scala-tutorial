import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/** Inverted Index - Basis of Search Engines */
object InvertedIndex5b {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("output/crawl"),
      CommandLineOptions.outputPath("output/inverted-index"),
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

    val name = "Inverted Index (5b)"
    val conf = new SparkConf().
      setMaster(master).
      setAppName(name).
      set("spark.app.id", name)   // To silence Metrics warning.
    val sc = new SparkContext(conf)

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma,
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path").toString is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path")) map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          // If any of these were returned, you could filter them out below.
          ("", "")
      }

      if (!quiet) println(s"Writing output to: $out")

      // Split on non-alphabetci sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      input
        .flatMap {
          // all lines are two-tuples; extract the path and text into variables
          // named "path" and "text".
          case (path, text) =>
            // If we don't trim leading whitespace, the regex split creates
            // an undesired leading "" word!
            text.trim.split("""[^\p{IsAlphabetic}]+""") map (word => (word, path))
        }
        .map {
          // We're going to use the (word, path) tuple as a key for counting
          // all of them that are the same. So, create a new tuple with the
          // pair as the key and an initial count of "1".
          case (word, path) => ((word, path), 1)
        }
        .reduceByKey{    // Count the equal (word, path) pairs, as before
          (count1, count2) => count1 + count2
        }
        // Instead, we could use placeholder "_" for each argument: .reduceByKey(_ + _)
        .map {           // Rearrange the tuples; word is now the key we want.
          case ((word, path), n) => (word, (path, n))
        }
        .groupByKey      // There is a also a more general groupBy
        // reformat the output; make a string of each group,
        // a sequence, "(path1, n1) (path2, n2), (path3, n3)..."
        .mapValues(iterator => iterator.mkString(", "))
        // mapValues is like the following map, but more efficient, as we skip
        // pattern matching on the key ("word"), etc.
        // .map {
        //   case (word, seq) => (word, seq.mkString(", "))
        // }
        .saveAsTextFile(out)
    } finally {
      // This is a good time to look at the web console again:
      if (! quiet) {
        println("""
          |========================================================================
          |
          |    Before closing down the SparkContext, open the Spark Web Console
          |    http://localhost:4040 and browse the information about the tasks
          |    run for this example.
          |
          |    When finished, hit the <return> key to exit.
          |
          |========================================================================
          """.stripMargin)
        Console.in.read()
      }
      sc.stop()
    }

    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: For each output record, sort the list of (path, n) tuples by n,
    //   descending.
    // Exercise: Try you own set of text files. First run Crawl5a to generate
    //   the "web crawl" data.
    // Exercise (hard): Try combining some of the processing steps or reordering
    //   steps to make it more efficient.
    // Exercise (hard): As written the output data has an important limitation
    //   for use in a search engine. Really common words, like "a", "an", "the",
    //   etc. are pervasive. There are two tools to improve this. One is to
    //   filter out so-called "stop" words that aren't useful for the index.
    //   The second is to use a variation of this algorithm called "term
    //   frequency-inverse document frequency" (TF-IDF). Look up this algorithm
    //   and implement it.
  }
}
