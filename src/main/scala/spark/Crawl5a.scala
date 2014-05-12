package spark

import spark.util.{CommandLineOptions, Timestamp}
import java.io.{File, FilenameFilter}
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/** 
 * Simulate a web crawl to prep. data for InvertedIndex5b.
 */
object Crawl5a {
  def main(args: Array[String]) = {

    // I extracted the command-line processing code in WordCount3 into
    // a separate utility class. Here, I'm using named arguments for clarity,
    // but this is not required:
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/enron-spam-ham"),
      CommandLineOptions.outputPath("output/crawl"),
      CommandLineOptions.master("local"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Crawl (5a)")

    try {
      val files_rdds = ingestFiles(argz("input-path").toString, sc)
      val names_contents = files_rdds map {
        // fold to convert lines into a long, space-separated string.
        // keyBy to generate a new RDD with schema (file_name, file_contents)
        case (file, rdd) => (file.getName, rdd.fold("")(_ + " " + _))
      }
      // Because we want to read this output with the InvertedIndex script, we
      // don't append a timestamp. That means if you run this script repeatedly,
      // you'll need to delete the output directory from the previous run first.
      // val now = Timestamp.now()
      // val out = s"${args("output-path").toString}-$now"
      val out = argz("output-path").toString
      println(s"Writing output to: $out")
      sc.makeRDD(names_contents).saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }

  /** 
   * Walk the directory tree. Read each file into an RDD, returning a sequence
   * of them. Skip a README, if any, and any "hidden" files (".*") that are
   * returned as directory contents.
   */
  def ingestFiles(inpath: String, sc: SparkContext): Seq[(File,RDD[String])] = {
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

    toRDDs(new File(inpath), Seq.empty[(File,RDD[String])])
  }

  // EXERCISE: Try passing the input path argument in a different format:
  //   sbt run-main spark.Crawl5a -i 'data/enron-spam-ham/ham100/*,data/enron-spam-ham/spam100/*' -o output/crawl2
  //   How is the output different in output/crawl2?
}