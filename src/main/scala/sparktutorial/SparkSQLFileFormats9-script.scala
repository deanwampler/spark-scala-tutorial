// HiveSQLFileFormats10-script.scala - A Scala script that demonstrates support
// for various file formats.

import org.apache.spark.sql.SQLContext
import util.{Verse, FileUtil}

/**
 * Example of SparkSQL's support for Parquet and JSON.
 */

val inputDir   = "data/kjvdat.txt"

import sqlContext.sql

// Read the verses into a table, just as we did in SparkSQL8.scala:
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
val versesRDD = sc.textFile(inputDir).flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println(s"Unexpected line: $line")
    Nil // or use Seq.empty[Verse]. It will be eliminated by flattening.
}
val verses = sqlContext.createDataFrame(versesRDD)
verses.registerTempTable("kjv_bible")

// Save as a Parquet file:
val parquetDir = "output/parquet"
println(s"Saving 'verses' as a Parquet file to $parquetDir.")
FileUtil.rmrf(parquetDir)
verses.write.parquet(parquetDir)

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

// Work with JSON!
// Requires each JSON "document" to be on a single line.
// Let's first right some JSON (check the files!).
val jsonDir = "output/json"
println(s"Saving 'verses' as a JSON file to $jsonDir.")
FileUtil.rmrf(jsonDir)
verses.write.json(jsonDir)
val versesJSON = sqlContext.read.json(jsonDir)
versesJSON.show

// Not needed if you're using the actual Spark Shell and our configured sbt
// console command.
// sc.stop()

