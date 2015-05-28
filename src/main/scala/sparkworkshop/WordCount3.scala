import com.typesafe.sparkworkshop.util.{ CommandLineOptions, FileUtil, TextUtil }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._

/**
 * This second implementation of Word Count that makes the following changes:
 * <ol>
 * <li>A simpler approach is used for the algorithm.</li>
 * <li>A CommandLineOptions library is used.</li>
 * <li>The handling of the per-line data format is refined.</li>
 * <li>We show how to use Kryo serialization for better efficiency.</li>
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
    val in     = argz("input-path")
    val out    = argz("output-path")
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    // Let's use Kryo serialization. Here's how to set it up.
    val conf = new SparkConf().setMaster(master).setAppName("Word Count (3)")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // If the data had a custom type, we would want to register it. Kryo already
    // handles common types, like String, which is all we use here:
    // conf.registerKryoClasses(Array(classOf[MyCustomClass]))

    val sc = new SparkContext(conf)

    try {
      // Load the input text, convert each line to lower case, then split
      // into fields:
      //   book|chapter|verse|text
      // Keep only the text. The output is an RDD.
      // Note that calling "last" on the split array is robust against lines
      // that don't have the delimiter, if any.
      // (Don't cache this time, as we're making a single pass through the data.)
      val input = sc.textFile(in)
        .map(line => TextUtil.toText(line)) // also converts to lower case

      // Split on non-alphabetic sequences of character as before.
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      val wc2a = input
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .countByValue() // Returns a Map[T, Long]

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
