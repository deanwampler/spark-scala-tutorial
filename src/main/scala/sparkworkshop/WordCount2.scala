// package com.foo.bar    // You could put the code in a package...

import com.typesafe.sparkworkshop.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}
// Implicit conversions, such as methods defined in
// org.apache.spark.rdd.PairRDDFunctions
// (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions)
import org.apache.spark.SparkContext._

/**
 * First implementation of Word Count. We'll run this one locally, to get a
 * sense for the development process outside Hadoop. The subsequent exercises
 * will run in Hadoop.
 * Scala makes the Singleton Design Pattern "first class". The "object" keyword
 * declares an class with a single instance that the runtime will create itself.
 * You put definitions in objects that would be declared static in Java, like
 * "main".
 */
object WordCount2 {
  def main(args: Array[String]): Unit = {

    // The first argument specifies the "master" (see the tutorial notes).
    // The second argument is a name for the job.
    val sc = new SparkContext("local", "Word Count (2)", new SparkConf())

    // Put the "stop" inside a finally clause, so it's invoked even when
    // something fails that forces an abnormal termination.
    try {

      val out = "output/kjv-wc2"
      // Deleting old output (if any) DO NOT DO IN PRODUCTION!!      // Deleting old output (if any)
      FileUtil.rmrf(out)

      // Load the King James Version of the Bible, then convert
      // each line to lower case, creating an RDD.
      val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)

      // We could cache the RDD in memory for fast, repeated access, but
      // we don't have to do this here because we're only going to make one
      // pass through it. So, caching here could be inefficient.
      // input.cache

      // Split on non-alphabetic sequences of characters. Note the regex used;
      // the similar, non-alphanumeric alternative """\W+""", does not work well
      // with non-UTF8 character sets!
      // Since each single line is converted to a sequence of words, we use
      // flatMap to flatten the sequence of sequences into a single sequence of
      // words. These words are then mapped into tuples that add a count of 1
      // for the word. Note the simplistic approach to tokenization; we just
      // split on any run of characters that isn't alphabetic.
      // Finally, reduceByKey functions like a SQL "GROUP BY" followed by
      // a count of the elements in each group. The words are the keys and
      // values are the 1s, which are added together, effectively counting
      // the occurrences of each word.
      val wc = input
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .map(word => (word, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        // .reduceByKey(_ + _)

      // Save, but it actually writes Hadoop-style output; to a directory,
      // with a _SUCCESS marker (empty) file, the data as a "part" file,
      // and checksum files.
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()      // Stop (shut down) the context.
    }

    // Exercise: Use other versions of the Bible:
    //   The data directory contains similar files for the Tanach (t3utf.dat - in Hebrew),
    //   the Latin Vulgate (vuldat.txt), the Septuagint (sept.txt - Greek)
    // Exercise: See the Scaladoc page for `OrderedRDDFunctions`:
    //   http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.OrderedRDDFunctions
    //   Sort the output by word, try both ascending and descending.
    //   Note this can be expensive for large data sets!
    // Exercise: Take the output from the previous exercise and count the number
    //   of words that start with each letter of the alphabet and each digit.
    // Exercise (Hard): Sort the output by count. You can't use the same
    //   approach as in the previous exercise. Hint: See RDD.keyBy
    //   (http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD)
    //   What's the most frequent word that isn't a "stop word".
    // Exercise (Hard): Group the word-count pairs by count. In other words,
    //   All pairs where the count is 1 are together (i.e., just one occurrence
    //   of those words was found), all pairs where the count is 2, etc. Sort
    //   ascending or descending. Hint: Is there a method for grouping?
    // Exercise (Thought Experiment): Consider the size of each group created
    //   in the previous exercise and the distribution of those sizes vs. counts.
    //   What characteristics would you expect for this distribution? That is,
    //   which words (or kinds of words) would you expect to occur most
    //   frequently? What kind of distribution fits the counts (numbers)?
  }
}
