import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Second implementation of Word Count that makes the following changes:
 * <ol>
 * <li>A simpler approach is used for the algorithm.</li>
 * <li>A CommandLineOptions library is used.</li>
 * <li>The handling of the per-line data format is refined.</li>
 * </ol>
 */
object WordCount3 {
  def main(args: Array[String]): Unit = {
    // I extracted command-line processing code into a separate utility class,
    // an illustration of how it's convenient that we can mix "normal" code
    // with "big data" processing code.
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-wc3"),
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

    val sc = new SparkContext(master, "Word Count (3)")

    try {
      // Load the input text, convert each line to lower case, then split
      // into fields:
      //   book|chapter|verse|text
      // Keep only the text. The output is an RDD.
      // Note that calling "last" on the split array is robust against lines
      // that don't have the delimiter, if any.
      val input = sc.textFile(argz("input-path"))
        .map(line => line.toLowerCase.split("\\s*\\|\\s*").last)

      // Cache the RDD in memory for fast, repeated access.
      // You don't have to do this and you shouldn't unless the data IS reused.
      // Otherwise, you'll use RAM inefficiently.
      input.cache

      // Split on non-alphanumeric sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      val wc2a = input
        .flatMap(line => line.split("""\W+"""))
        .countByValue()  // Returns a Map[T, Long]

      // ... and convert back to an RDD for output, with one "slice".
      // First, convert to a comma-separated string. When you call "map" on
      // a Map, you get 2-tuples for the key-value pairs. You extract the
      // first and second elements with the "_1" and "_2" methods, respectively.
      val wc2b = wc2a.map(key_value => s"${key_value._1},${key_value._2}").toSeq
      val wc2 = sc.makeRDD(wc2b, 1)

      if (!quiet) println(s"Writing output to: $out")
      wc2.saveAsTextFile(out)

    } finally {
      sc.stop()
    }

    // Exercise: Try different arguments for the input and output.
    //   NOTE: I've observed 0 output for some small input files!
    // Exercise: Don't discard the book names.
  }
}
