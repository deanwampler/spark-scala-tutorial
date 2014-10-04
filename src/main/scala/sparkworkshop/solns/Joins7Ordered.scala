import com.typesafe.sparkworkshop.util.CommandLineOptions
import com.typesafe.sparkworkshop.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/**
 * Joins7 - Perform joins of datasets.
 * Implements the exercise to restore the original order of the verses.
 */
object Joins7Ordered {
  def main(args: Array[String]) = {

    /** The "dictionary" of book abbreviations to full names */
    val abbrevsFile = "data/abbrevs-to-names.tsv"
    val abbrevs = Opt(
      name   = "abbreviations",
      value  = abbrevsFile,
      help   = s"-a | --abbreviations  path The dictionary of book abbreviations to full names (default: $abbrevsFile)",
      parser = {
        case ("-a" | "--abbreviations") +: path +: tail => (("abbreviations", path), tail)
      })

    // New: A hard-coded map of the book name abbreviation and its index,
    // counting from 1 (which doesn't matter, as only relative numbers are used).
    // We will use this to reorder the records correctly after the join "mixes
    // them up". Note that we could have put this data into a file that's either
    // read "locally" or into an RDD. If you use such a local file in a real
    // cluster, there is a spark argument to copy files like this around the
    // cluster. Finally, if your dataset is split into multiple partitions this
    // technique will ensure sorting within the partition, but not globally
    // across the output files.
    // Note that by hard-coding this map, this version of the app will NOT
    // work with other texts. In fact, it will through an exception when we call
    // "bookToIndex.get(book)" below if other texts are used.
    val bookToIndex = Map(
      "Gen" -> 1,
      "Exo" -> 2,
      "Lev" -> 3,
      "Num" -> 4,
      "Deu" -> 5,
      "Jos" -> 6,
      "Jdg" -> 7,
      "Rut" -> 8,
      "Sa1" -> 9,
      "Sa2" -> 10,
      "Kg1" -> 11,
      "Kg2" -> 12,
      "Ch1" -> 13,
      "Ch2" -> 14,
      "Ezr" -> 15,
      "Neh" -> 16,
      "Est" -> 17,
      "Job" -> 18,
      "Psa" -> 19,
      "Pro" -> 20,
      "Ecc" -> 21,
      "Sol" -> 22,
      "Isa" -> 23,
      "Jer" -> 24,
      "Lam" -> 25,
      "Eze" -> 26,
      "Dan" -> 27,
      "Hos" -> 28,
      "Joe" -> 29,
      "Amo" -> 30,
      "Oba" -> 31,
      "Jon" -> 32,
      "Mic" -> 33,
      "Nah" -> 34,
      "Hab" -> 35,
      "Zep" -> 36,
      "Hag" -> 37,
      "Zac" -> 38,
      "Mal" -> 39,
      "Mat" -> 40,
      "Mar" -> 41,
      "Luk" -> 42,
      "Joh" -> 43,
      "Act" -> 44,
      "Rom" -> 45,
      "Co1" -> 46,
      "Co2" -> 47,
      "Gal" -> 48,
      "Eph" -> 49,
      "Phi" -> 50,
      "Col" -> 51,
      "Th1" -> 52,
      "Th2" -> 53,
      "Ti1" -> 54,
      "Ti2" -> 55,
      "Tit" -> 56,
      "Plm" -> 57,
      "Heb" -> 58,
      "Jam" -> 59,
      "Pe1" -> 60,
      "Pe2" -> 61,
      "Jo1" -> 62,
      "Jo2" -> 63,
      "Jo3" -> 64,
      "Jde" -> 65,
      "Rev" -> 66)

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      abbrevs,
      CommandLineOptions.outputPath("output/kjv-joins-ordered"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Joins (7)")
    try {
      // Load one of the religious texts, don't convert each line to lower case
      // this time, then extract the fields in the "book|chapter|verse|text" format
      // used for each line, creating an RDD. However, note that the logic used
      // to split the line will work reliably even if the delimiters aren't present!
      // Note also the output nested tuple. Joins only work for RDDs of
      // (key,value) tuples
      val input = sc.textFile(argz("input-path").toString)
        .map { line =>
          val ary = line.split("\\s*\\|\\s*")
          (ary(0), (ary(1), ary(2), ary(3)))
        }

      // The abbreviations file is tab separated, but we only want to split
      // on the first space (in the unlikely case there are embedded tabs
      // in the names!)
      val abbrevs = sc.textFile(argz("abbreviations").toString)
        .map{ line =>
          val ary = line.split("\\s+", 2)
          (ary(0), ary(1).trim)  // I've noticed trailing whitespace...
        }

      // Cache both RDDs in memory for fast, repeated access.
      input.cache
      abbrevs.cache

      // Join on the key, the first field in the tuples; the book abbreviation.

      val verses = input.join(abbrevs)

      if (input.count != verses.count) {
        println(s"input count, ${input.count}, doesn't match output count, ${verses.count}")
      }

      // Order the records (again) and project out the final, flattened data
      // we want:
      //   fullBookName|chapter|verse|text
      // To do the ordering, we first use our map of book abbreviations to
      // position indices to construct a sortable key, which happens to be a
      // tuple of three integers, BUT ONLY AFTER WE CONVERT the chapter and
      // verse from strings to integers. The text and full book name are the
      // record's value part. Note that we now drop the abbreviated name,
      // as it's no longer needed.
      val verses2 = verses
        .map {
          case (book, ((chapter, verse, text), fullBookName)) =>
            // ====================== Key: =======================,
              ((bookToIndex.get(book), chapter.toInt, verse.toInt),
            // ====== Value: ======
               (text, fullBookName))
        }
        .sortByKey()
        // Finally, reformat and drop the temporary index:
        .map {
          case ((index, chapter, verse), (text, fullBookName)) =>
            (fullBookName, chapter, verse, text)
        }

      val out = argz("output-path").toString
      if (argz("quiet").toBoolean == false)
        println(s"Writing output to: $out")
      verses2.saveAsTextFile(out)
    } finally {
      sc.stop()
    }

    // Exercise: Try different sacred text files.
    // Exercise: Try outer joins (see http://spark.apache.org/docs/1.0.0/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions).
    // Exercise (hard): The output does NOT preserve the original order of the
    //   verses! This is a consequence of how joins are implemented ("co-groups").
    //   Fix the ordering. Here is one approach:
    //   Compute (in advance??) a map from book names (or abbreviations) to
    //   an index (e.g., Gen -> 1, Exo -> 2, ...). Use this to construct a
    //   sort key containing the book index, chapter, and verse. Note that
    //   the chapter and verse will be strings when extracted from the file,
    //   so you must convert them to integers (i.e., "x.toInt"). Finally,
    //   project out the full book name, chapter, verse, and text.
  }
}
