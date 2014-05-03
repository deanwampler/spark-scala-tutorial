package spark

import spark.util.{CommandLineOptions, Timestamp}
import spark.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** NGrams6 - Find the ngrams in a corpus */
object NGrams6 {
  def main(args: Array[String]) = {

    def count(value: String): Opt = Opt(
      name   = "count",
      value  = value,
      help   = "-c | --count  N     The number of NGrams to compute (default: $value)",
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
      help   = "-n | --ngrams  S     The NGrams match string (default: $value)",
      parser = {
        case ("-n" | "--ngrams") +: s +: tail => (("ngrams", s), tail)
      })
    
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("stdout"), 
      CommandLineOptions.master("local"),
      count("100"),
      ngrams("I love %"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "NGrams (6)")
    val ngramsStr = argz("ngrams").toString.toLowerCase
    // Note that the replacement strings use Scala's triple quotes; necessary
    // to ensure that the final string is "\w+" and "\s+" for the reges.
    val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
    val n = argz("count").toInt
    try {
      // Load the input data. Note that NGrams across line boundaries are not
      // supported by this implementation.

      val ngramz = sc.textFile(argz("input-path").toString)
        .flatMap {
          line => ngramsRE.findAllMatchIn(line.toLowerCase).map(_.toString)
        }
        .map(ngram => (ngram, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        .sortByKey(false)  // false for descending
        .take(n)           // "LIMIT n"

      // Save to a file, but because we no longer have an RDD, we have to use
      // good 'ol Java File IO. Note that the output specifier is now a file, 
      // not a directory as before, the format of each line will be diffierent,
      // and the order of the output will not be the same, either. 
      val (out, close) = determineOut(argz("output-path"), ngramz.size)
      ngramz foreach {
        case (ngram, count) => out.println("%30s\t%d".format(ngram, count))
      }
      // WARNING: Without this close statement, it appears the output stream is 
      // not completely flushed to disk!
      close()
    } finally {
      sc.stop()
    }

    // Exercise: Try different ngrams and input texts. Note that you can specify
    //           a regular expression, e.g.,
    //           run-main spark.NGrams6 --ngrams 'I (lov|hat)ed? %'

    // Exercise: Sort the output by the words. How much overhead does this add?

    // Exercise: For each output record, sort the list of (path, n) tuples by n.

    // Exercise: Try you own set of text files. First run Crawl5a to generate
    // the "web crawl" data.

    // Exercise (hard): Try combining some of the processing steps or reordering
    // steps to make it more efficient.
  }

  import java.io._
  def determineOut(outpath: String, size: Int): (PrintWriter, () => Unit) = {
    if (outpath != "stdout") {
      val now = Timestamp.now()
      val outpathnow = s"$outpath-$now"
      println(s"Writing output ($size records) to $outpathnow")
      val out = new PrintWriter(outpathnow)
      (out, () => out.close())
    } else {
      println(s"Writing output ($size records) to the console")
      (new PrintWriter(System.out), () => {})
    }
  }
}
