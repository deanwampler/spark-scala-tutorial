// Intro1.scala - A Scala script will use interactively in the Spark Shell.
// Script files can't be compiled in the same way as normal code files, so
// the SBT build is configured to ignore this file.

// If you're using the Spark Shell, the following two import statements and
// and construction of the SparkContext variable "sc" are done automatically
// by the shell. In this tutorial, we don't download a full Spark distribution
// with the $SPARK_HOME/bin/spark-shell. Instead, we'll use SBT's "console"
// task, but we'll configure it to use the same commands that spark-shell use.
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// val sc = new SparkContext("local[*]", "Intro (1)")

// Load the King James Version of the Bible, then convert
// each line to lower case, creating an RDD.
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)

// Cache the data in memory for faster, repeated retrieval.
input.cache

// Find all verses that mention "sin".
val sins = input.filter(line => line.contains("sin"))

// The () are optional in Scala for no-argument methods
val count = sins.count()         // How many sins?
val array = sins.collect()       // Convert the RDD into a collection (array)
array.take(20) foreach println   // Take the first 20, and print them 1/line.
sins.take(20) foreach println    // ... but we don't have to "collect" first;
                                 // we can just use foreach on the RDD.

// Create a separate filter function instead and pass it as an argument to the
// filter method. "filterFunc" is a value that's a function of type
// String to Boolean.
val filterFunc: String => Boolean =
    (s:String) => s.contains("god") || s.contains("christ")
// Equivalent, due to type inference:
//  s => s.contains("god") || s.contains("christ")

// Filter the sins for the verses that mention God or Christ (lowercase)
val sinsPlusGodOrChrist  = sins filter filterFunc
// Count how many we found. (Note we dropped the parentheses after "count")
val countPlusGodOrChrist = sinsPlusGodOrChrist.count

// Let's do "Word Count", where we load a corpus of documents, tokenize them into
// words and count the occurrences of all the words.

// Define a helper method to look at the data. First we need to import the RDD
// type:
import org.apache.spark.rdd.RDD

def peek(rdd: RDD[_], n: Int = 10): Unit = {
  println("=====================")
  rdd.take(n).foreach(println)
  println("=====================")
}

// First, recall what "input" is:
input
peek(input)

// Now split into words on anything that isn't an alphanumeric character:
val words = input.flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
peek(words)
val wordGroups = words.groupBy(word => word)
peek(wordGroups)
val wordCounts1 = wordGroups.map( word_group => (word_group._1, word_group._2.size))
peek(wordCounts1)

// Another way to do the previous: Use "pattern matching" to break up the tuple
// into two fields.
val wordCounts2 = wordGroups.map{ case (word, group) => (word, group.size) }
peek(wordCounts2)

// But there is actually an even easier way. Note that we aren't modifying the
// "keys" (the words), so we can use a convenience function mapValues, where only
// the value part (2nd tuple element) is passed to the anonymous function and
// the keys are retained:
val wordCounts3 = wordGroups.mapValues(group => group.size)
peek(wordCounts3)

wordCounts3.saveAsTextFile("output/kjv-wc-groupby")

// Not needed if you're using the actual Spark Shell and our configured sbt
// console command.
// sc.stop()

// Exercise: Try different filters. The filter function could match on a
//   regular expression, for example. Note also the line format in the input
//   text files. It would be easy to filter on book of the bible, etc.
// Exercise: Try different sacred texts in the "data" directory, download other
//   texts from http://www.sacred-texts.com/, or just use any other texts you have.
