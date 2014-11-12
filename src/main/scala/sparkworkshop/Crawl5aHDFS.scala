import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import java.io.{File, FilenameFilter}
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Simulate a web crawl to prep. data for InvertedIndex5b.
 * Crawl uses <code>SparkContext.wholeTextFiles</code> to read the files
 * in a directory hierarchy and return a single RDD with records of the form:
 *    (file_name, file_contents)
 * Hence this call does in one step what we do in several steps in <code>Crawl5a</code>.
 * Unfortunately, it appears that <code>SparkContext.wholeTextFiles</code> doesn't
 * work for local file systems. Hence, we have two implementations of Crawl.
 *
 * WARNING. To use this version of Crawl5a, you have to stage the HAM and SPAM
 * files differently in HDFS. See the Tutorial or top-level README for details.
 */
object Crawl5aHDFS {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/enron-spam-ham/"),
      CommandLineOptions.outputPath("output/crawl"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz    = options(args.toList)
    val master  = argz("master").toString
    val quiet   = argz("quiet").toBoolean
    val out     = argz("output-path").toString
    val in      = argz("input-path").toString

    val sc = new SparkContext(master, "CrawlHDFS (5a)")

    try {
      val files_contents = sc.wholeTextFiles(argz("input-path"))
      if (!quiet) println(s"Writing ${files_contents.count} lines to: $out")
      // The schema now is (fileName: String, text: String), but the text
      // still contains all the linefeeds, which would mess up InvertedIndex,
      // since we use the simple text file format. So, we have to remove these
      // embedded linefeeds before writing the output.
      files_contents.map{
        case (id, text) => (id, text.replaceAll("""\s*\n\s*""", " "))
      }.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
