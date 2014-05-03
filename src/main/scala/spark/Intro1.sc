// Intro1.sc - A Scala script will use interactively in the Spark Shell.
// (The .sc extension is used so SBT doesn't try compiling it.)

// The SparkContext is created by the spark shell automatically.
// The val keyword declares a read-only variable: "value".
// val sc = new SparkContext(...)

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
array.take(100) foreach println  // Take first 100, and print them 1/line.

// Create a separate filter function instead and pass it as an argument to the 
// filter method. "filterFunc" is a value that's a function of String to Boolean
val filterFunc: String => Boolean = 
    (s:String) => s.contains("god") || s.contains("christ") 
// Equivalent, due to type inference:
//  s => s.contains("god") || s.contains("christ") 

val sinsPlusGodOrChrist  = sins filter filterFunc
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
val arrayPlusGodOrChrist = sinsPlusGodOrChrist.collect

// Prep for subsequent exercises; create an "output" directory
val output = new java.io.File("output")
if (output.exists == false) output.mkdir

// Exercise: Try different filters. 
// Exercise: Try different files in the "data" directory. 
