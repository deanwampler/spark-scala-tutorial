// Additional code for the exercise in SparkSQL8-script.scala
// to join the data with the abbreviations-to-name mapping.
// This code assumes the "kjv_bible" and abbrevs_to_names temp
// tables are already defined. In other words, you can't run this
// script by itself.

val counts = sql("""
  SELECT name, count FROM (
    SELECT book, COUNT(*) as count FROM kjv_bible GROUP BY book) bc
  JOIN abbrevs_to_names an ON bc.book = an.abbrev
  """).coalesce(1)
counts.registerTempTable("counts")
counts.printSchema
counts.queryExecution
counts.show(100)  // print all the lines; there are 66 books in the KJV.

// DataFrame version:
val countsDF = verses.groupBy("book").count().
  join(abbrevNames, verses("book") === abbrevNames("abbrev")).
  select("name", "count").coalesce(1)
countsDF.show(100)
