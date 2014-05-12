import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive.LocalHiveContext
import spark.util.Verse

/** 
 * Example of Accessing Hive Tables directly. SQL, using the KJV Bible text. 
 * Writes "query" results to the console, rather than a file.
 */

val sc = new SparkContext("local", "Hive SQL (10)")

// The analog of SQLContext we used in the previous exercise is Hive context
// that starts up an instance of Hive where metadata is stored locally in
// an in-process "metastore", which is stored in the ./metadata directory.
// Similarly, the ./warehouse directory is used for the regular data
// "warehouse".
// If you have a Hive installation, you might try using HiveContext instead to
// connect to your existing metastore.
val hiveContext = new LocalHiveContext(sc, "warehouse")
import hiveContext._   // Make methods local, as for SQLContext

// The "hql" method let's us run the full set of Hive SQL statements.
// Let's create a table for our KJV data. Note that Hive lets us specify
// the field separator for the data and we can make a table "external", 
// which means that Hive won't "own" the data, but instead just read it
// from the location we specify.
hql("""
  |CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
  |  book    STRING,
  |  chapter INT,
  |  verse   INT,
  |  text    STRING)
  |  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  |  LOCATION 'data/kjvdat.txt'
  |""".stripMargin)

println("Run the same GROUP BY we ran before:")
hql("""
  |SELECT * FROM (
  |  SELECT book, COUNT(*) AS count FROM bible GROUP BY book) bc
  |WHERE bc.book <> '';
  |""".stripMargin).collect.foreach(println)

println("Run the 'God' query we ran before:")
hql("SELECT * FROM bible WHERE text LIKE '%God%';").collect().foreach(println)

// Don't forget this when you're done:
// sc.stop()

// Exercise: Repeat some of the exercises we did for SparkSQL9:
//  1. Sort the output by the words. How much overhead does this add?
//  2. Try a JOIN with the "abbrevs_to_names" data to convert the book 
//       abbreviations to full titles.
//  3. Play with the SchemaRDD DSL.
//  4. Try some of the other sacred text data files.
