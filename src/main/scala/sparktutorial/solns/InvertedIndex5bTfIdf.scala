package sparktutorial.solns

import util.{CommandLineOptions, FileUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/** Inverted Index - Basis of Search Engines */
object InvertedIndex5bTfIdf {
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

    val name = "Inverted Index - TfIdf (5b)"
    val spark = SparkSession.builder.
      master("local[*]").
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      getOrCreate()
    val sc = spark.sparkContext

    try {
      // Load the input "crawl" data, where each line has the format:
      //   (document_id, text)
      // First remove the outer parentheses, split on the first comma,
      // trim whitespace from the name (we'll do it later for the text)
      // and convert the text to lower case.
      // NOTE: The args("input-path").toString is a directory; Spark finds the correct
      // data files, part-NNNNN.
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path")).map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine =>
          Console.err.println(s"Unexpected line: $badLine")
          // If any of these were returned, you could filter them out below.
          ("", "")
      }  // RDD[(String,String)] of (path,text) pairs

      if (!quiet) println(s"Writing output to: $out")

      // The TF-IDF algorithm works in the following way. For word in every document we need
      // to compute Term Frequency (TF) which is computed as a number of times every word
      // occurs in a document divided by a size of this document. Then for every word we need to compute
      // Inverse Document Frequency (IDF) which is a logarithm of number number of documents divided
      // by a number of documents with this word. When we are done computing both TF and IDF values for
      // every word we need to compute TF-IDF value for every word in every document. TF-IDF is just a
      // multiplication of TF and IDF values for corresponding word.

      // We compute TF first which results in a dataset in format (word, (path, TF-value))
      val tf = input
        .flatMap {
          case (path, text) =>

            // Split on non-alphabetical sequences of character as before.
            val words = text.trim.split("""[^\p{IsAlphabetic}]+""")
            words
              // Compute map (word, count)
              .groupBy(identity)
              .mapValues(_.length)
              // Convert every (word, count) pair into (word, (path, TF-value))
              .toSeq
              .map{
                case (word, count) => (word, (path, count.toDouble / words.length))
              }
        } // RDD[(String, (String, Double))]


      // Calculate number of emails for IDF computations
      val documentsCount = input.count
      // Now we compute IDF part
      val idf = input
        // Return unique pairs (word, path) for every email. Even if a word occurs
        // in an email more then once we need to return (word, path) pair only once
        // to correctly computer IDF
        .flatMap {
          case (path, text) =>
            // Split text as before
            val words = text.trim.split("""[^\p{IsAlphabetic}]+""")
            words
              // Convert to set to eliminate duplications in words
              .toSet[String]
              .map((word) => (word, path))
        } // RDD[(String, Double)]
        // Since every pair (word, path) occurs only once "groupByKey" will transform
        // the dataset to format (word, <list of emails where "word" occurred>)
        .groupByKey() // RDD[(String, Iterable[String])]
        .map {
          // "paths" contains a list of documents where "word" occurred
          case (word, paths) =>
            // Return (word, IDF-value)
            (word, Math.log(documentsCount.toDouble / paths.size))
        } // RDD[(String, Double)]

      // Now we need to combine TF an IDF parts. To do this we need to combine
      // tuples from both dataset with the same word. To do this we can use
      // "join" method that will transfer two dataset into one with format (word, ((path, tfValue), idfValue)
      // where (path, tfValue) comes from the "tf" dataset and idfValue comes from the "idf" dataset
      tf.join(idf)
        .map {
          case (word, ((path, tfValue), idfValue)) =>
            // Compute TF-IDF value for every word
            (word, (path, tfValue * idfValue))
        } // RDD[(String, (String, Double))]
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
      spark.stop()
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

