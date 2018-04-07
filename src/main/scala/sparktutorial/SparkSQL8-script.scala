// Adapted from SparkSQL8, but written as a script for easy use with the
// spark-shell command.
import util.Verse
import org.apache.spark.sql.DataFrame

// For HDFS:
// val inputRoot = "hdfs://my_name_node_server:8020"
val inputRoot = "."
val inputPath = s"$inputRoot/data/kjvdat.txt"

// We already have sqlContext from the Spark Shell and SBT console.

// Regex to match the fields separated by "|".
// Also strips the trailing "~" in the KJV file.
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
// Use flatMap to effectively remove bad lines.
val versesRDD = sc.textFile(inputPath).flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println(s"Unexpected line: $line")
    Nil // or use Seq.empty[Verse]. It will be eliminated by flattening.
}

// Create a DataFrame and register as a temporary "table".
val verses = sqlContext.createDataFrame(versesRDD)
verses.registerTempTable("kjv_bible")
verses.cache()
// print the 1st 20 lines. Pass an integer argument to show a different number
// of lines:
verses.show()
verses.show(100)

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

// Use GroupBy and column aliasing.
val counts = sql("SELECT book, COUNT(*) as count FROM kjv_bible GROUP BY book")
counts.show(100)  // print the 1st 100 lines, but there are only 66 books/records...

// Exercise: Sort the output by the book names. Sort by the counts.

// Use "coalesce" when you have too many small partitions. The integer
// passed to "coalesce" is the number of output partitions (1 in this case).
val counts1 = counts.coalesce(1)
val nPartitions  = counts.rdd.partitions.size
val nPartitions1 = counts1.rdd.partitions.size
println(s"counts.count (can take a while, # partitions = $nPartitions):")
println(s"result: ${counts.count}")
println(s"counts1.count (usually faster, # partitions = $nPartitions1):")
println(s"result: ${counts1.count}")

// DataFrame version:
val countsDF = verses.groupBy("book").count()
countsDF.show(100)
countsDF.count

// Exercise: Sort the last output by the words, by counts. How much overhead does this add?

// Aggregations, a la data warehousing:
verses.groupBy("book").agg(
  max(verses("chapter")),
  max(verses("verse")),
  count(verses("*"))
).sort($"count(1)".desc, $"book").show(100)

// Alternative way of referencing columns in "verses":
verses.groupBy("book").agg(
  max($"chapter"),
  max($"verse"),
  count($"*")
).sort($"count(1)".desc, $"book").show(100)

// With just a single column, cube and rollup make less sense,
// but in a bigger dataset, you could do cubes and rollups, too.
verses.cube("book").agg(
  max($"chapter"),
  max($"verse"),
  count($"*")
).sort($"count(1)".desc, $"book").show(100)

verses.rollup("book").agg(
  max($"chapter"),
  max($"verse"),
  count($"*")
).sort($"count(1)".desc, $"book").show(100)

// Map a field to a method to apply to it, but limited to at most
// one method per field.
verses.rollup("book").agg(Map(
  "chapter" -> "max",
  "verse" -> "max",
  "*" -> "count"
)).sort($"count(1)".desc, $"book").show(100)

// Exercise: Try a JOIN with the "abbrevs_to_names" data to convert the book
// abbreviations to full titles, using either a SQL query or the DataFrame API.
// (See solns/SparkSQL-..-script.scala)
// Here is some setup code to load and parse the dataset:

val abbrevsNamesPath = s"$inputRoot/data/abbrevs-to-names.tsv"

case class Abbrev(abbrev: String, name: String)

val abbrevNamesRDD = sc.textFile(abbrevsNamesPath).flatMap { line =>
  val ary=line.split("\t")
  if (ary.length != 2) {
    Console.err.println(s"Unexpected line: $line")
    Nil // or use Seq.empty[Abbrev]. It will be eliminated by flattening.
  } else {
    Seq(Abbrev(ary(0), ary(1)))
  }
}
val abbrevNames = sqlContext.createDataFrame(abbrevNamesRDD)
abbrevNames.registerTempTable("abbrevs_to_names")

// Exercise: Play with other methods in the DataFrame DSL.
