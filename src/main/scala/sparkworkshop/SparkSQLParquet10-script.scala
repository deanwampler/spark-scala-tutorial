// HiveSQLParquet10.scala - A Scala script will use interactively in the Spark Shell.
// Script files can't be compiled in the same way as normal code files, so
// the SBT build is configured to ignore this file.

// Not needed when using spark-shell or our sbt console setup:
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import com.typesafe.sparkworkshop.util.Verse

/**
 * Example of SparkSQL's support for Parquet. This script requires a Hadoop
 * distribution, due to dependencies used for the Parquet support. See the
 * SparkSQL documentation for details.
 * For that reason, it doesn't assume you'll use the SBT console.
 * Instead, use Spark's "spark-submit" script instead.
 */

val inputDir   = "data/kjvdat.txt"
val parquetDir = "output/parquet"

// Not needed with spark-shell:
// val master = "yarn-client"
// val sc = new SparkContext(master, "Spark SQL Parquet (10)")
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._

import sqlContext.sql

// Read the verses into a table, just as we did in SparkSQL9.scala:
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
val versesRDD = sc.textFile(inputDir) flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println(s"Unexpected line: $line")
    Seq.empty[Verse]  // Will be eliminated by flattening.
}
val verses = sqlContext.createDataFrame(versesRDD)
verses.registerTempTable("kjv_bible")

// Save as a Parquet file:
println(s"Saving 'verses' as a Parquet file to $parquetDir.")
println("NOTE: You'll get an error if the directory already exists!")
verses.write.save(parquetDir)

// Now read it back and use it as a table:
println(s"Reading in the Parquet file from $parquetDir:")
val verses2 = sqlContext.read.parquet(parquetDir)
verses2.registerTempTable("verses2")
verses2.show

// Run a SQL query against the table:
println("Using the table from Parquet File, select Jesus verses:")
val jesusVerses = sql("SELECT * FROM verses2 WHERE text LIKE '%Jesus%'")
println("Number of Jesus Verses: "+jesusVerses.count())
jesusVerses.show

// Not needed if you're using the actual Spark Shell and our configured sbt
// console command.
// sc.stop()

