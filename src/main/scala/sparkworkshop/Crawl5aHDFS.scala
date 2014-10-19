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
 */
object Crawl5aHDFS {
  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/enron-spam-ham"),
      CommandLineOptions.outputPath("output/crawl"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz    = options(args.toList)
    val master  = argz("master")
    val quiet   = argz("quiet").toBoolean
    val out     = argz("output-path")
    val in      = argz("input-path")
    if (master.startsWith("local")) {
      println("ERROR: Crawl5aHDFS only supports Hadoop execution.")
      sys.exit(1)
    }

    val sc = new SparkContext(master, "CrawlHDFS (5a)")

    try {
      val files_contents = sc.wholeTextFiles(argz("input-path"))
      if (!quiet) println(s"Writing ${files_contents.count} lines to: $out")
      files_contents.saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
