// HiveSQL11.scala - A Scala script will use interactively in the Spark Shell.
// Script files can't be compiled in the same way as normal code files, so
// the SBT build is configured to ignore this file.

// Not needed when using spark-shell or our sbt console setup:
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.hive.LocalHiveContext
import com.typesafe.sparkworkshop.util.Verse

/**
 * Example of Accessing Hive Tables directly. SQL, using the KJV Bible text.
 * Writes "query" results to the console, rather than a file.
 * This script requires a Hadoop distribution, due to Hive's dependencies
 * See the SparkSQL documentation for details.
 * For that reason, it doesn't assume you'll use the SBT console.
 * Instead, use Spark's "spark-submit" script instead.
 */

val master = "local[2]"

val sc = new SparkContext(master, "Hive SQL (11)")

// The analog of SQLContext we used in the previous exercise is Hive context
// that starts up an instance of Hive where metadata is stored locally in
// an in-process "metastore", which is stored in the ./metadata directory.
// Similarly, the ./warehouse directory is used for the regular data
// "warehouse".
// If you have a Hive installation, you might try using HiveContext instead to
// connect to your existing metastore.
val hiveContext = new LocalHiveContext(sc)
import hiveContext._   // Make methods local, as for SQLContext

// The "hql" method let's us run the full set of Hive SQL statements.
// Let's create a table for our KJV data. Note that Hive lets us specify
// the field separator for the data and we can make a table "external",
// which means that Hive won't "own" the data, but instead just read it
// from the location we specify.
//
// NOTES ('cause the API is quirky!):
// 1. You must first copy "data/kjvdat.txt" to an empty "/tmp/kjv" directory,
//    because Hive will expect the LOCATION below to be an absolute directory
//    path and it will read all the files in it.
//    If you are on Windows and not running Cygwin, pick a suitable location
//    and change the LOCATION line to the correct absolute path.
// 2. Omit the trailing "/" in the LOCATION path.
// 3. Omit semicolons at the end of the HQL (Hive SQL) string.
// 4. The query results are in an RDD; use collect.print*...

println("Create the 'external' kjv Hive table:")
hql("""
  CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
    book    STRING,
    chapter INT,
    verse   INT,
    text    STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/data'""")

println("How many records?")
hql("SELECT COUNT(*) FROM kjv").collect.foreach(println)

println("Print the first few records:")
hql("SELECT * FROM kjv LIMIT 10").collect.foreach(println)

println("Run the same GROUP BY we ran before:")
hql("""
  SELECT * FROM (
    SELECT book, COUNT(*) AS count FROM kjv GROUP BY book) bc
  WHERE bc.book != ''""")
.collect.foreach(println)

println("Run the 'God' query we ran before:")
hql("SELECT * FROM kjv WHERE text LIKE '%God%'").collect().foreach(println)

// Drop the table. We're using Hive's embedded Derby SQL "database" for the
// "metastore" (Table metadata, etc.) See the "metastore" subdirectory you
// now have! Because the table is EXTERNAL, we only delete the metadata, but
// not the table data itself.
println("Drop the table.")
hql("DROP TABLE kjv").collect().foreach(println)

// Not needed if you're using the actual Spark Shell and our configured sbt
// console command.
// sc.stop()

// Exercise: Repeat some of the exercises we did for SparkSQL9:
//  1. Sort the output by the words. How much overhead does this add?
//  2. Try a JOIN with the "abbrevs_to_names" data to convert the book
//       abbreviations to full titles.
//  3. Try other Hive queries. See https://cwiki.apache.org/confluence/display/Hive/LanguageManual
//  4. Play with the SchemaRDD DSL.
//  5. Try some of the other sacred text data files.
