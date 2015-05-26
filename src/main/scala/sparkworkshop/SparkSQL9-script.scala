// Adapted from SparkSQL9, but written as a script for easy use with the
// spark-shell command.
import com.typesafe.sparkworkshop.util.Verse
import org.apache.spark.sql.DataFrame

// For HDFS:
// val inputRoot = "hdfs://my_name_node_server:8020"
val inputRoot = "."
val inputPath = s"$inputRoot/data/kjvdat.txt"

// Dump a DataFrame to the console when running locally.
// By default, it prints the first 100 lines of output, but you can call dump
// with another number as the second argument to change that.
def dump(df: DataFrame, n: Int = 100) =
  df.take(n).foreach(println) // Take the first n lines, then print them.

// The following are already invoked when we start sbt `console` or `spark-shell`
// in the Spark distribution:
// val sqlContext = new SQLContext(sc)
// import sqlContext.implicits._

// Regex to match the fields separated by "|".
// Also strips the trailing "~" in the KJV file.
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
// Use flatMap to effectively remove bad lines.
val versesRDD = sc.textFile(inputPath) flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println(s"Unexpected line: $line")
    Seq.empty[Verse]  // Will be eliminated by flattening.
}

// Create a DataFrame and register as a temporary "table".
val verses = sqlContext.createDataFrame(versesRDD)
verses.registerTempTable("kjv_bible")
verses.cache()
// print the 1st 20 lines (Use dump(verses), defined above, for more lines)
verses.show()

import sqlContext.sql  // for convenience

val godVerses = sql("SELECT * FROM kjv_bible WHERE text LIKE '%God%'")
println("The query plan:")
godVerses.queryExecution   // Compare with godVerses.explain(true)
println("Number of verses that mention God: "+godVerses.count())
godVerses.show()

// Use the DataFrame API:
val godVersesDF = verses.filter(verses("text").contains("God"))
println("The query plan:")
godVersesDF.queryExecution
println("Number of verses that mention God: "+godVersesDF.count())
godVersesDF.show()

// NOTE: The basic SQL dialect currently supported doesn't permit
// column aliasing, e.g., "COUNT(*) AS count". This makes it difficult
// to write the following query result to Parquet, for example.
// Nor does it appear to support WHERE clauses in some situations.
val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
dump(counts)  // print the 1st 100 lines, but there are only 66 books/records...

// "Coalesce" all partitions into 1 partition. Otherwise, there are
// 100s of partitions output from the last query. This isn't terrible when
// calling dump, but watch what happens when you run the following two counts:
println("counts.count (takes a while):")
println(s"result: ${counts.count}")
val counts1 = counts.coalesce(1)
println("counts1.count (fast!!):")
println(s"result: ${counts1.count}")

// DataFrame version:
val countsDF = verses.groupBy("book").count()
dump(countsDF)
countsDF.count
val countsDF1 = countsDF.coalesce(1)
countsDF1.count

// Exercise: Sort the output by the words. How much overhead does this add?
// Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
//   abbreviations to full titles. (See solns/SparkSQL-..-script.scala)
// Exercise: Play with the DataFrame DSL.
// Exercise: Try some of the other sacred text data files.
