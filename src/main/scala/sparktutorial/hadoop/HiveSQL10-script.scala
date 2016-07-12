// HiveSQL10-script.scala - Explore SparkSQL's Hive integration.
// In general `HiveContext.sql(...)` supports arbitrary DDL statements,
// including table creation, modification, and deletion. Those statements
// work best when connected with a regular Hive metastore. Here, we'll just
// demonstrate using the DataFrame API to read some data and construct a
// partitioned Hive table from it.

// We'll ignore the sqlContext that we create with the SBT console:
// The analog of SQLContext we used in the previous exercise is Hive context
// that starts up an instance of Hive where metadata is stored locally in
// an in-process "metastore", which is stored in the ./metastore_db directory.
// Similarly, the ./warehouse directory is used for the regular data
// "warehouse". HiveContext reads your hive-site.xml file to determine properties
// like the hostname and port.
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
println("""
    ================================

    Ignore warnings such as:
      "ObjectStore: Failed to get database default, returning NoSuchObjectException"
      "ParquetRecordReader: Can not initialize counter ..."

    While you shouldn't see them in production configurations, they are okay for now.

    ================================
  """)

val hiveContext = new HiveContext(sc)

// Show the currently defined tables
def tables(): Unit = {
  hiveContext.tables.show
}
tables  // should be empty

// Where will tables and metadata be written?
val metadata = "hive.metastore.warehouse.dir"
println(s"$metadata:\t${hiveContext.getConf(metadata)}")

// Wrap the "sql" method.
def sql2(title: String, query: String, n: Int = 100): Unit = {
  println(title)
  println(s"Running query: $query")
  hiveContext.sql(query).show(truncate = false, numRows = n)
}

// Load the KJV verses. Adapted from SparkSQL8-script.scala; see comments there.
import util.Verse

val inputPath = "./data/kjvdat.txt"
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
val versesRDD = sc.textFile(inputPath).flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println(s"Unexpected line: $line")
    Seq.empty[Verse]  // Will be eliminated by flattening.
}

// Create a DataFrame and register as a "permanent" Hive table.
// Notice the statement that will be printed out which shows you the directory
// where the data files are written. Look in that directory and you'll see
// subdirectories like "book=Act", "book=Amo". These are the partitions, by
// book, and each one will contain a single Parquet file.
val verses = hiveContext.createDataFrame(versesRDD)
verses.cache()
verses.write.format("parquet").partitionBy("book").saveAsTable("kjv_bible")

// print the 1st 20 lines. Pass an integer argument to show a different number
// of lines:
verses.show()
verses.show(100)

sql2("Describe the table kjv_bible:", "DESCRIBE FORMATTED kjv_bible")

// Look for a [31102] line at or near the end of the output:
sql2("How many records?", "SELECT COUNT(*) FROM kjv_bible")

sql2("Print the first few records:", "SELECT * FROM kjv_bible LIMIT 10")

sql2("Run the same GROUP BY we ran before:",
  "SELECT book, COUNT(*) AS count FROM kjv_bible GROUP BY book")

sql2("Run the 'God' query we ran before:",
  "SELECT * FROM kjv_bible WHERE text LIKE '%God%'")

// Exercise: Repeat some of the exercises we did for SparkSQL8:
//  1. Sort the output by the words. How much overhead does this add?
//  2. Try a JOIN with the "abbrevs_to_names" data to convert the book
//       abbreviations to full titles.
//  3. Try other Hive queries. See https://cwiki.apache.org/confluence/display/Hive/LanguageManual
//  4. Play with the SchemaRDD DSL.
//  5. Try some of the other sacred text data files.

// Drop the table and database. We're using Hive's embedded Derby SQL "database"
// for the "metastore" (Table metadata, etc.). See the "metastore" subdirectory
// you now have! Because the table is EXTERNAL, we only delete the metadata, but
// not the table data itself.
sql2("Drop the table.", "DROP TABLE kjv_bible")

tables
