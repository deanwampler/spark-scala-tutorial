// Adapted from SparkSQL9, but written as a script for easy use with the
// spark-shell command.
import com.typesafe.sparkworkshop.util.Verse
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

// Dump an RDD to the console when running locally.
// By default, it prints the first 100 lines of output, but you can call dump
// with another number as the second argument to change that.
def dump(rdd: RDD[_], n: Int = 100) =
  rdd.take(n).foreach(println) // Take the first n lines, then print them.

val sqlc = new SQLContext(sc)
import sqlc._

// Regex to match the fields separated by "|".
// Also strips the trailing "~" in the KJV file.
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
// Use flatMap to effectively remove bad lines.
val verses = sc.textFile(argz("input-path")) flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println("Unexpected line: $line")
    Seq.empty[Verse]  // Will be eliminated by flattening.
}

// Register the RDD as a temporary "table".
// The following expression invokes several "implicit" conversions and
// methods that we imported through sqlc._ The actual method is
// defined on org.apache.spark.sql.SchemaRDDLike, which also has a method
// "saveAsParquetFile" to write a schema-preserving Parquet file.
verses.registerTempTable("kjv_bible")
verses.cache()
dump(verses)  // print the 1st 100 lines

val godVerses = sql("SELECT * FROM kjv_bible WHERE text LIKE '%God%'")
println("Number of verses that mention God: "+godVerses.count())
dump(godVerses)  // print the 1st 100 lines


// NOTE: The basic SQL dialect currently supported doesn't permit
// column aliasing, e.g., "COUNT(*) AS count". This makes it difficult
// to write the following query result to Parquet, for example.
// Nor does it appear to support WHERE clauses in some situations.
val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
  // Collect all partitions into 1 partition. Otherwise, there are 100s
  // output from the last query!
  .coalesce(1)
dump(counts)  // print the 1st 100 lines

// Exercise: Sort the output by the words. How much overhead does this add?
// Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
//   abbreviations to full titles.
// Exercise: Play with the SchemaRDD DSL.
// Exercise: Try some of the other sacred text data files.
