import com.typesafe.sparkworkshop.util.FileUtil
import org.apache.spark.{SparkConf, SparkContext}
// Implicit conversions, such as methods defined in
// [org.apache.spark.rdd.PairRDDFunctions](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions)
import org.apache.spark.SparkContext._

/**
 * First implementation of Word Count.
 * Also implements the group by exercise.
 */
object WordCount2GroupBy {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local", "Word Count (2) - Group By", new SparkConf())

    try {

      val out = "output/kjv-wc2-group-by-count"
      println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)

      // Load the King James Version of the Bible, then convert
      // each line to lower case, creating an RDD.
      val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)

      // Cache the RDD in memory for fast, repeated access.
      // You don't hae to do this and you shouldn't unless the data IS reused.
      // Otherwise, you'll use RAM inefficiently.
      input.cache

      // Split on non-alphabetic sequences of characters. Since each single
      // line is converted to a sequence of words, we use flatMap to flatten
      // the sequence of sequences into a single sequence of words.
      // These words are then mapped into tuples that add a count of 1
      // for the word.
      // Finally, reduceByKey functions like a SQL "GROUP BY" followed by
      // a count of the elements in each group. The words are the keys and
      // values are the 1s, which are added together, effectively counting
      // the occurrences of each word.
      val wc = input
        .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
        .map(word => (word, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        .groupBy(tuple => tuple._2)   // group by the counts!
        .sortByKey(true)              // also try descending (false)

      // Save, but it actually writes Hadoop-style output; to a directory,
      // with a _SUCCESS marker (empty) file, the data as a "part" file,
      // and checksum files.
      // Note that when you look at the output, you'll see some very
      // long lines for the many words that have very low counts (e.g., 1),
      // while very frequent words (counts in the 1000s) usually don't overlap,
      // e.g., "for" has 8971 occurrences, while "unto" has 8997, so they have
      // nearly the same frequency, but not the exact same, so they aren't
      // grouped together.
      println(s"Writing output to: $out")
      wc.saveAsTextFile(out)
    } finally {
      sc.stop()
    }

    // Exercise: See the Scaladoc page for `OrderedRDDFunctions`:
    //   http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.OrderedRDDFunctions
    //   Sort the output by word, try both ascending and descending.
    //   Note this can be expensive!
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
