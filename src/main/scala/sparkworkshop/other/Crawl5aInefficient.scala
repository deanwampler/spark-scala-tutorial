package com.typesafe.sparkworkshop.other

import com.typesafe.sparkworkshop.util.{CommandLineOptions, Timestamp}
import java.io.{File, FilenameFilter}
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * This is an alternative implementation of Crawl5a. It is more elegant
 * in some ways, but it is MUCH less inefficient. It creates a separate output
 * file for each input file, rather than aggregating into a small number of
 * output files. The extra overhead makes this version run considerably slower
 * and the subsequent InvertedIndex5b runs a lot slower, too.
 */
object Crawl5aInefficient {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/enron-spam-ham"),
      CommandLineOptions.outputPath("output/crawl-inefficient"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Crawl (5a - inefficient)")

    try {
      // Because we want to read this output with the InvertedIndex script, we
      // don't append a timestamp. That means if you run this script repeatedly,
      // you'll need to delete the output directory from the previous run first.
      // HOWEVER, if you run the InvertedIndex script on this output, you will
      // have to specify the different output directory used here:
      // val now = Timestamp.now()
      // val out = s"${argz("output-path").toString}-$now"
      val out = s"${argz("output-path")}"
      if (argz("quiet").toBoolean == false)
        println(s"Writing output to: $out")

      ingestFiles(argz("input-path").toString, sc).saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }

  /**
   * Walk the directory tree. Return an RDD where each record is the file
   * path and the contents, all on one line.
   * Skip a README, if any, and any "hidden" files (".*") that are
   * returned as directory contents.
   */
  def ingestFiles(inpath: String, sc: SparkContext): RDD[(String,String)] = {
    val filter = new FilenameFilter {
      val skipRE = """^(\..*|README).*""".r
      def accept(directory: File, name: String): Boolean = name match {
        case skipRE(ignore) => false
        case _ => true
      }
    }

    def fileToRDD(file: File): RDD[(String,String)] = {
      val path = file.getPath
      sc.textFile(path)   // RDD[String], where each line is a String
      .map (line => (path, line))  // RDD[(String,String)], prefix the path
      // Reduce over the path and concatenate the lines.
      .reduceByKey ((line1, line2) => line1 + " " + line2) // RDD[(String,String)]
    }

    // A more scalable approach is to use an org.apache.spark.Accumulable
    // shared variable. The implementation here is synchronous.
    def toRDDs(file: File, accum: Seq[RDD[(String,String)]]): Seq[RDD[(String,String)]] =
      if (file.isDirectory)
        file.listFiles(filter).foldLeft(accum) ( (acc, f) => toRDDs(f, acc) )
      else fileToRDD(file) +: accum

    // Return the sequence of RDDs then union them into one.
    sc.union(toRDDs(new File(inpath), Seq.empty[RDD[(String,String)]]))
  }
}