package spark

import spark.util.{CommandLineOptions, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** Inverted Index - Basis of Search Engines */
object InvertedIndex5b {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("output/crawl"),
      CommandLineOptions.outputPath("output/inverted-index"),
      CommandLineOptions.master("local"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Inverted Index (5b)")

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma, 
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path").toString is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path").toString) map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case line => 
          Console.err.println("Unexpected line: $line")
          // If any of these were returned, you could filter them out below.
          ("", "")  
      }

      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")

      // Split on non-alphanumeric sequences of character as before. 
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      input
        .flatMap { 
          case (path, text) => text.split("""\W+""") map (word => (word, path))
        }
        .map { 
          case (word, path) => ((word, path), 1) 
        }
        .reduceByKey{
          case (count1, count2) => count1 + count2
        }
        .map {
          case ((word, path), n) => (word, (path, n)) 
        }
        .groupBy {
          case (word, (path, n)) => word
        }
        .map {
          case (word, seq) => 
            val seq2 = seq map {
              case (redundantWord, (path, n)) => (path, n)
            }
            (word, seq2.mkString(", "))
        }
        .saveAsTextFile(out)
    } finally {
      sc.stop()
    }

    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: For each output record, sort the list of (path, n) tuples by n.
    // Exercise: Try you own set of text files. First run Crawl5a to generate
    //   the "web crawl" data.
    // Exercise (hard): Try combining some of the processing steps or reordering
    //   steps to make it more efficient.
    // Exercise (hard): As written the output data has an important limitation 
    //   for use in a search engine. Really common words, like "a", "an", "the", 
    //   etc. are pervasive. There are two tools to improve this. One is to filter
    //   out so-called "stop" words that aren't useful for the index. The second
    //   is to use a variation of this algorithm called "term frequency-inverse 
    //   document frequency" (TF-IDF). Look up this algorithm and implement it.
  }
}
