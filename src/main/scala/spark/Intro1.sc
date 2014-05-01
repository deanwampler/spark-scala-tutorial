// Intro1.sc - A Scala script will use interactively in the Spark Shell.
// (The .sc extension is used so SBT doesn't try compiling it.)

// Created by the spark shell automatically.
// val sc = new SparkContext(...)

// Load the King James Version of the Bible, then convert 
// each line to lower case, creating an RDD.
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache

val sins = input.filter(line => line.contains("sin"))
val count = sins.count()      // The () are optional
val array = sins.collect()    // ditto
array.take(100) foreach println

// Create a separate filter function instead and pass it as an argument to the 
// filter method.
val filterFunc: String => Boolean = 
  s => s.contains("god") || s.contains("christ") 
val sinsPlusGodOrChrist  = sins filter filterFunc
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
val arrayPlusGodOrChrist = sinsPlusGodOrChrist.collect

// Prep for subsequent exercises:
// 1. Create an "output" directory
val output = new java.io.File("output")
if (output.exists == false) output.mkdir
