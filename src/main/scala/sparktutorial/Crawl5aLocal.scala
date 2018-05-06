import util.{CommandLineOptions, FileUtil}
import java.io.{File, FilenameFilter}
import scala.io.Source

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Simulate a web crawl to prep. data for InvertedIndex5b. This version works
 * with a local file system. It's no longer used in the exercises, but we left
 * it here in case you find it useful. This version has one slight advantage
 * over <code>Cralwwl5a</code>; it will crawl an arbitrarily deep tree, not just
 * one level.
 * Crawl is designed to read the files in a directory tree. It uses the
 * file name as the key and the contents as the value. This script also
 * uses Java's local filesystem I/O library, which means the script can't
 * work in Hadoop as written, because HDFS is not POSIX compliant. However,
 * see the companion program, <code>Crawl5a</code>, which uses a different Spark API
 * call to ingest the data as needed.
 */
object Crawl5aLocal {
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
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    } else {
      println("ERROR: Crawl5a only supports local mode execution.")
      sys.exit(1)
    }

    val name = "CrawlLocal (5a)"
    val spark = SparkSession.builder.
      master(master).
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      getOrCreate()
    val sc = spark.sparkContext

    try {
      val files_rdds = ingestFiles(in, sc)
      val names_contents = files_rdds.map {
        // fold to convert lines into a long, space-separated string.
        // keyBy to generate a new RDD with schema (file_name, file_contents)
        case (file, rdd) => (file.getName, rdd.fold("")(_ + " " + _))
      }

      // Convert back to an RDD and write the results.
      if (!quiet) println(s"Writing output to: $out")
      sc.makeRDD(names_contents).saveAsTextFile(out)

    } finally {
      spark.stop()
    }
  }

  /**
   * Walk the directory tree. Read each file into an RDD, returning a sequence
   * of them. Skip a README, if any, and any "hidden" files (".*") that are
   * returned as directory contents.
   */
  protected def ingestFiles(path: String, sc: SparkContext): Seq[(File,RDD[String])] = {
    val filter = new FilenameFilter {
      val skipRE = """^(\..*|README).*""".r
      def accept(directory: File, name: String): Boolean = name match {
        case skipRE(ignore) => false
        case _ => true
      }
    }

    // A more scalable approach is to use an org.apache.spark.Accumulable
    // shared variable. The implementation here is synchronous.
    def toRDDs(file: File, accum: Seq[(File,RDD[String])]): Seq[(File,RDD[String])] =
      if (file.isDirectory) {
        // Process the directory (recursively) and fold its results in...
        file.listFiles(filter).foldLeft(accum) ( (acc, f) => toRDDs(f, acc) )
      } else {
        (file, sc.textFile(file.getPath)) +: accum
      }

    toRDDs(new File(path), Seq.empty[(File,RDD[String])])
  }

  // EXERCISE: Try passing the input path argument in a different format:
  //   sbt run-main spark.Crawl5a -i 'data/enron-spam-ham/ham100/*,data/enron-spam-ham/spam100/*' -o output/crawl2
  //   How is the output different in output/crawl2?
}
