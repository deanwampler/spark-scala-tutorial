import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Second, simpler implementation of Word Count.
 * Solution to the exercise that orders by word length. We'll add the length
 * to the output, as the 2nd field after the word itself.
 */
object WordCount3SortByWordLength {
  def main(args: Array[String]) = {

    // I extracted command-line processing code into a separate utility class,
    // an illustration of how it's convenient that we can mix "normal" code
    // with "big data" processing code.
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-wc3-word-length"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master").toString
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path").toString
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    val sc = new SparkContext(master, "Word Count (3)")

    try {
      // Load the King James Version of the Bible, then convert
      // each line to lower case, creating an RDD.
      val input = sc.textFile(argz("input-path").toString).map(line => line.toLowerCase)

      // Cache the RDD in memory for fast, repeated access.
      // You don't have to do this and you shouldn't unless the data IS reused.
      // Otherwise, you'll use RAM inefficiently.
      input.cache

      // Split on non-alphanumeric sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      // Note that this implementation would not be a good choice at very
      // large scale, because countByValue forces it to a single, in-memory
      // collection on one node, after which we do further processing.
      // Try redoing it with the WordCount3 "reduceByKey" approach. What
      // changes are required to the rest of the script?
      val wc2a = input
        .flatMap(line => line.split("""\W+"""))
        .countByValue()  // Returns a Map[T, Long]
        .toVector        // Extract into a sequence that can be sorted.
        .map{ case (word, count) => (word, word.length, count) } // add length
        .sortBy{ case (_, length, _) => -length }  // sort descending, ignore
                                                   // 1st, 3rd tuple elements!

      // ... and convert back to an RDD for output, with one "slice".
      // First, convert to a comma-separated string. When you call "map" on
      // a Map, you get 2-tuples for the key-value pairs. You extract the
      // first and second elements with the "_1" and "_2" methods, respectively.
      val wc2b = wc2a.map(key_value => s"${key_value._1},${key_value._2}").toSeq
      val wc2 = sc.makeRDD(wc2b, 1)

      if (!quiet) println(s"Writing output to: $out")
      wc2.saveAsTextFile(out)

    } finally {
      // Stop (shut down) the context.
      sc.stop()
    }

    // Exercise: Try different arguments for the input and output.
    //   NOTE: I've observed 0 output for some small input files!
  }
}
