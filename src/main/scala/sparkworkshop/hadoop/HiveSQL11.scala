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

// Logic to determine the user name from the system environment.
val user = sys.env.get("USER") match {
  case Some(user) => user
  case None =>
    println("ERROR: USER environment variable isn't defined. Using root!")
    "root"
}

// Not needed with spark-shell:
// val master = "yarn-client"
// val sc = new SparkContext(master, "Hive SQL (11)")

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
// We wrap it in a helper method for conveniently dumping the results:
def hql2(title: String, query: String, n: Int = 100): Unit = {
  println(title)
  println(s"Running query: $query")
  hql(query).take(n).foreach(println)
}

// Let's create a database for our work:
hql2("Create a work database:", "CREATE DATABASE work")
hql2("Use the work database:", "USE work")

// Let's create a table for our KJV data. Note that Hive lets us specify
// the field separator for the data and we can make a table "external",
// which means that Hive won't "own" the data, but instead just read it
// from the location we specify.
//
// NOTES ('cause the API is quirky!):
// 1. Make sure the directory is correct for the location of the data in HDFS.
//    It must be an absolute path.
// 2. Omit the trailing "/" in the LOCATION path.
// 3. Omit semicolons at the end of the HQL (Hive SQL) string.
// 4. The query results are in an RDD.

// A copy of the the KJV data in its own directory, because Hive only
// works with directories for locations.
hql2("Create the 'external' kjv Hive table:", s"""
  CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
    book    STRING,
    chapter INT,
    verse   INT,
    text    STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/user/$user/data/hive-kjv'""")

hql2("Describe the table kjv:", "DESCRIBE FORMATTED kjv")

// Look for a [31102] line at or near the end of the output:
hql2("How many records?", "SELECT COUNT(*) FROM kjv")

hql2("Print the first few records:", "SELECT * FROM kjv LIMIT 10")

hql2("Run the same GROUP BY we ran before:", """
  SELECT * FROM (
    SELECT book, COUNT(*) AS count FROM kjv GROUP BY book) bc
  WHERE bc.book != ''""")

hql2("Run the 'God' query we ran before:",
  "SELECT * FROM kjv WHERE text LIKE '%God%'")

// Exercise: Repeat some of the exercises we did for SparkSQL9:
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
hql2("Drop the table.", "DROP TABLE kjv")
hql2("Drop the database.", "DROP DATABASE work")

// Not needed if you're using the actual Spark Shell and our configured sbt
// console command.
// sc.stop()
