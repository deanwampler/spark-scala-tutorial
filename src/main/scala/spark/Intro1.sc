// Intro1.sc - A Scala script will use interactively in the Spark Shell.
// (The .sc extension is used so SBT doesn't try compiling it.)

// If you're using the Spark Shell, the following two import statements
// and construction of the SparkContext variable "sc" are done automatically
// by the shell. In this tutorial, we don't download a full Spark distribution
// with the $SPARK_HOME/bin/spark-shell, so we'll use SBT's "console" task:
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext_
// The val keyword declares a read-only variable: "value".
val sc = new SparkContext("local", "Intro (1)")

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

// Create a separate filter function instead and pass it as an argument to the 
// filter method. "filterFunc" is a value that's a function of String to Boolean
val filterFunc: String => Boolean = 
    (s:String) => s.contains("god") || s.contains("christ") 
// Equivalent, due to type inference:
//  s => s.contains("god") || s.contains("christ") 

// Filter the sins for the verses that mention God or Christ (lowercase)
val sinsPlusGodOrChrist  = sins filter filterFunc
// Count how many we found.
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
// Convert the RDD to an in-memory collection. What's its type?
val arrayPlusGodOrChrist = sinsPlusGodOrChrist.collect

// Prep for subsequent exercises; create an "output" directory
val output = new java.io.File("output")
if (output.exists == false) output.mkdir

// Exercise: Try different filters. The filter function could match on a 
// regular expression, for example. Note also the line format in the input text
// files. It would be easy to filter on book of the bible, etc.
// Exercise: Try different sacred texts in the "data" directory, download other
// texts from http://www.sacred-texts.com/, or just use any other texts you have.
