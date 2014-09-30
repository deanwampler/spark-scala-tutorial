package com.typesafe.sparkworkshop

import com.typesafe.sparkworkshop.util.{CommandLineOptions, Timestamp, Verse}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

/**
 * Example of Spark SQL, using the KJV Bible text.
 * Writes "query" results to the console, rather than a file.
 */
object SparkSQLParquet10 {

  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.quiet)

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Spark SQL Parquet (10)")
    val sqlContext = new SQLContext(sc)
    import sqlContext._    // Make its methods accessible.

    try {

      // Now read it back and use it as a table:
      println("Reading in the Parquet file from output/verses.parquet:")
      val verses2 = sqlContext.parquetFile("output/verses.parquet")
      verses2.registerAsTable("verses2")

      // Run a SQL query against the table:
      println("Using the table from Parquet File, select Jesus verses:")
      val jesusVerses = sql("SELECT * FROM verses2 WHERE text LIKE '%Jesus%';")
      println("Number of Jesus Verses: "+jesusVerses.count())

      // Use the LINQ-inspired DSL that's provided by the class
      // org.apache.spark.sql.SchemaRDD that's used implicitly:
      println("GROUP BY using the LINQ-style query API:")
      // Seems like a bug that you have to import this explicitly:
      import org.apache.spark.sql.catalyst.expressions.Sum

      verses2.groupBy('book)(Sum('verse) as 'count).orderBy('count.desc)
        .collect().foreach(println)

    } finally {
      sc.stop()
    }

    // Exercise: Sort the output by the words. How much overhead does this add?
    // Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
    //   abbreviations to full titles.
    // Exercise: Play with the SchemaRDD DSL.
    // Exercise: Try some of the other sacred text data files.
  }
}
