import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil, TextUtil}
import com.typesafe.sparkworkshop.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** NGrams6 - Find the ngrams in a corpus */
object NGrams6 {

  def main(args: Array[String]): Unit = {

    /** A function to generate an Opt for handling the count argument. */
    def count(value: String): Opt = Opt(
      name   = "count",
      value  = value,
      help   = s"-c | --count  N     The number of NGrams to compute (default: $value)",
      parser = {
        case ("-c" | "--count") +: n +: tail => (("count", n), tail)
      })

    /**
     * The NGram phrase to match, e.g., "I love % %" will find 4-grams that
     * start with "I love", and "% love %" will find trigrams with "love" as the
     * second word.
     * The "%" are replaced by the regex "\w+" and whitespace runs are replaced
     * with "\s+" to create a matcher regex.
     */
    def ngrams(value: String): Opt = Opt(
      name   = "ngrams",
      value  = value,
      help   = s"-n | --ngrams  S     The NGrams match string (default: $value)",
      parser = {
        case ("-n" | "--ngrams") +: s +: tail => (("ngrams", s), tail)
      })

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/ngrams"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet,
      count("100"),
      ngrams("% love % %"))

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    val sc = new SparkContext(master, "NGrams (6)")
    val ngramsStr = argz("ngrams").toLowerCase
    // Note that the replacement strings use Scala's triple quotes; necessary
    // to ensure that the final string is "\w+" and "\s+" for the reges.
    val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
    val n = argz("count").toInt
    try {

      /** Order the (ngram,count) pairs by count descending, ngram ascending. */
      object CountOrdering extends Ordering[(String,Int)] {
        def compare(a:(String,Int), b:(String,Int)) = {
          // Sort counts descending and so the test results are
          // predictable, then phrases ascending.
          val cntdiff = b._2 compare a._2
          if (cntdiff != 0) cntdiff else (a._1 compare b._1)
        }
      }

      // Load the input data. Note that NGrams across line boundaries are not
      // supported by this implementation.

      val ngramz = sc.textFile(argz("input-path"))
        .flatMap { line =>
          val text = TextUtil.toText(line) // also converts to lower case
          ngramsRE.findAllMatchIn(text).map(_.toString)
        }
        .map(ngram => (ngram, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        .takeOrdered(n)(CountOrdering)
        // The following would work instead if sorting by ngrams, since
        // the ngram is in the "key position", but it would be very
        // inefficient to do a total ordering, then take the top n.
        // takeOrdered(...)(...) should be used in both cases:
        // .sortByKey(false)  // false for descending
        // .take(n)           // "LIMIT n"

      // Format the output as a sequence of strings, then convert back to
      // an RDD for output.
      val outputLines = Vector(
        s"Found ${ngramz.size} ngrams:") ++ ngramz.map {
        case (ngram, count) => "%30s\t%d".format(ngram, count)
      }

      val output = sc.makeRDD(outputLines)  // convert back to an RDD
      if (!quiet) println(s"Writing output to: $out")
      output.saveAsTextFile(out)

    } finally {
      sc.stop()
    }

    // Exercise: Try different ngrams and input texts. Note that you can specify
    //   a regular expression, e.g.,
    //     run-main NGrams6 --ngrams "% (lov|hat)ed? % %"
    // Exercise (Hard): Read in many documents and retain the file, so you find
    // ngrams per document.
  }
}
