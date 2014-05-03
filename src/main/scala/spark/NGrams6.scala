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
      // CommandLineOptions.outputPath("output/ngrams"), // just write to the console 
      CommandLineOptions.master("local"),
      count("100"),
      ngrams("% love % %"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "NGrams (6)")
    val ngramsStr = argz("ngrams").toString.toLowerCase
    // Note that the replacement strings use Scala's triple quotes; necessary
    // to ensure that the final string is "\w+" and "\s+" for the reges.
    val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
    val n = argz("count").toInt
    try {

      object CountOrdering extends Ordering[(String,Int)] {
        def compare(a:(String,Int), b:(String,Int)) = 
          -(a._2 compare b._2)  // - so that it sorts descending
      }

      // Load the input data. Note that NGrams across line boundaries are not
      // supported by this implementation.

      val ngramz = sc.textFile(argz("input-path").toString)
        .flatMap {
          line => ngramsRE.findAllMatchIn(line.toLowerCase).map(_.toString)
        }
        .map(ngram => (ngram, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        // The following would work for sorting by ngrams:
        // .sortByKey(false)  // false for descending
        // .take(n)           // "LIMIT n"
        .takeOrdered(n)(CountOrdering)

      // Write to the console, but because we no longer have an RDD,
      //we have to use good 'ol Java File IO. Note that the output
      // specifier is now interpreted as a file, not a directory as before.
      println(s"Found ${ngramz.size} ngrams:")
      ngramz foreach {
        case (ngram, count) => println("%30s\t%d".format(ngram, count))
      }
    } finally {
      sc.stop()
    }

    // Exercise: Try different ngrams and input texts. Note that you can specify
    // a regular expression, e.g., 
    //   run-main spark.NGrams6 --ngrams '% (lov|hat)ed? % %'

    // Exercise (Hard): Read in many documents and retain the file, so you find
    // ngrams per document.
  }
}
