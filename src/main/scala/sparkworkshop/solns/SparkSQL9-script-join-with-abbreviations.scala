// Additional code for the exercise in SparkSQL9-script.scala
// to join the data with the abbreviations-to-name mapping.
// This code assumes definitions are already in scope, such as "inputRoot"
// and the "kjv_bible" temp table. In other words, you can't run it by itself.

val abbrevsNamesPath = s"$inputRoot/data/abbrevs-to-names.tsv"

case class Abbrev(abbrev: String, name: String)

val abbrevNames = sc.textFile(abbrevsNamesPath) flatMap { line =>
  val ary=line.split("\t")
  if (ary.length != 2) {
    Console.err.println("Unexpected line: $line")
    Seq.empty[Abbrev]
  } else {
    Seq(Abbrev(ary(0), ary(1)))
  }
}
abbrevNames.registerTempTable("abbrevs_to_names")

// SparkSQL doesn't yet support column aliases, like "COUNT(*) as count", 
// but it does assign a name to "columns" like this, which is "c1" in this case.
val counts = sql("""
  SELECT name, c1 FROM (
    SELECT book, COUNT(*) FROM kjv_bible GROUP BY book) bc
  JOIN abbrevs_to_names an ON bc.book = an.abbrev
  """).coalesce(1)
counts.registerTempTable("counts")
counts.printSchema
dump(counts)  // print all the lines; there are 66 books in the KJV.
