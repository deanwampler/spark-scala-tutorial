# Apache Spark: A Tutorial

## Introduction

This workshop demonstrates how to write and run [Apache Spark](http://spark.apache.org) *Big Data* applications. 

[Apache Spark](http://spark.apache.org) is a distributed computing system written in Scala and developed initially as a UC Berkeley research project for distributed data programming. It has grown in capabilities and it recently became a top-level [Apache project](http://spark.apache.org). 

We'll run our exercises "locally" on our laptops, which is very convenient for learning, development, and "unit" testing. However, there are several ways to run Spark clusters. There is even a *Spark Shell*, a customized version of the Scala REPL (read, eval, print loop shell), for interactive use.

## Why Spark?

By 2013, it became increasingly clear that a successor was needed for the venerable [Hadoop MapReduce](http://wiki.apache.org/hadoop/MapReduce) compute engine. MapReduce applications are difficult to write, but more importantly, MapReduce has significant performance limitations and it can't support event-streaming ("real-time") scenarios.

Spark was seen as the best, general-purpose alternative, so [Cloudera led the way](http://databricks.com/blog/2013/10/28/databricks-and-cloudera-partner-to-support-spark.html) in embracing Spark as a replacement for MapReduce. 

Spark is now officially supported in [Cloudera CDH5](http://blog.cloudera.com/blog/2014/04/how-to-run-a-simple-apache-spark-app-in-cdh-5/) and [MapR's distribution](http://blog.cloudera.com/blog/2014/04/how-to-run-a-simple-apache-spark-app-in-cdh-5/). Hortonworks has not yet announced whether or not they will support Spark natively, but [this page](http://spark.apache.org/docs/0.9.1/cluster-overview.html) in the Spark documentation discusses general techniques for running Spark with various versions of Hadoop, as well as other deployment scenarios.

## Spark Clusters

Let's briefly discuss the anatomy of a Spark standalone cluster, adapting [this discussion (and diagram) from the Spark documentation](http://spark.apache.org/docs/0.9.1/cluster-overview.html). Consider the following diagram:

![Spark Cluster](http://spark.apache.org/docs/0.9.1/img/cluster-overview.png)

Each program we'll write is a *Driver Program*. It uses a *SparkContext* to communicate with the *Cluster Manager*, either Spark's own manager or the corresponding management services provided by [Mesos](http://mesos.apache.org/) or [Hadoop's YARN](http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/YARN.html). The *Cluster Manager* allocates resources. An *Executor* JVM process is created on each worker node per client application. It manages local resources, such as the cache (see below) and it runs tasks, which are provided by your program in the form of Java jar files or Python scripts.

Because each application has its own executor process per node, applications can't share data through the *Spark Context*. External storage has to be used (e.g., the file system, a database, a message queue, etc.)

When possible, run the driver locally on the cluster to reduce network IO as it creates and manages tasks.

## Spark Deployment Options

Spark currently supports [three cluster managers](http://spark.apache.org/docs/0.9.1/cluster-overview.html):

* [Standalone](http://spark.apache.org/docs/0.9.1/spark-standalone.html) – A simple manager bundled with Spark for manual deployment and management of a cluster. It has some high-availability support, such as Zookeeper-based leader election of redundant master processes.
* [Apache Mesos](http://spark.apache.org/docs/0.9.1/running-on-mesos.html) – [Mesos](http://mesos.apache.org/) is a general-purpose cluster management system that can also run [Hadoop](http://hadoop.apache.org) and other services.
* [Hadoop YARN](http://spark.apache.org/docs/0.9.1/running-on-yarn.html) – [YARN](http://hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/YARN.html) is the [Hadoop](http://hadoop.apache.org) v2 resource manager.

Note that you can run Spark on a Hadoop cluster using any of these three approaches, but only YARN deployments truly integrate resource management between Spark and Hadoop jobs. Standalone and Mesos deployments within a Hadoop cluster require that you statically configure some resources for Spark and some for Hadoop, because Spark and Hadoop are unaware of each other in these configurations.

For information on using YARN, see [here](http://spark.apache.org/docs/0.9.0/running-on-yarn.html).

For information on using Mesos, see [here](http://spark.apache.org/docs/0.9.1/running-on-mesos.html) and [here](http://mesosphere.io/learn/run-spark-on-mesos/).

Spark also includes [EC2 launch scripts](http://spark.apache.org/docs/0.9.1/ec2-scripts.html) for running clusters on Amazon EC2.

## Resilient, Distributed Datasets

The data caching is one of the key reasons that Spark's performance is considerably better than the performance of MapReduce. Spark stores the data for the job in *Resilient, Distributed Datasets* (RDDs), where a logical data set is virtualized over the cluster. 

The user can specify that data in an RDD should be cached in memory for subsequent reuse. In contrast, MapReduce has no such mechanism, so a complex job requiring a sequence of MapReduce jobs will be penalized by a complete flush to disk of intermediate data, followed by a subsequent reloading into memory by the next job.

RDDs support common data operations, such as *map*, *flatmap*, *filter*, *fold/reduce*, and *groupby*. RDDs are resilient in the sense that if a "partition" of data is lost on one node, it can be reconstructed from the original source without having to start the whole job over again.

The architecture of RDDs is described in the research paper [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf).

## Spark 1.0.0

We will use Spark 1.0.0-RC5 so we can exploit the latest API features. The final release will be available soon. I'll update the workshop to final artifacts when they are available.

For now, we will use the following temporary locations for documentation and jar files:

* [Documentation](http://people.apache.org/~pwendell/spark-1.0.0-rc3-docs/).
* [Maven Repo](https://repository.apache.org/content/repositories/orgapachespark-1012/).

I recommend that you open the [Documentation link](http://people.apache.org/~pwendell/spark-1.0.0-rc3-docs/), as the getting-started guides, overviews, and reference pages (*Scaladocs*) will be useful. The Maven repo is used automatically by the build process.

## Building and Running

If you're using the [Activator UI](http://typesafe.com/activator), search for `activator-spark` and install it in the UI. The code is built automatically. 

If you grabbed this workshop from [Github](https://github.com/deanwampler/activator-spark), you'll need to install `sbt` and use a command line to build and run the applications. In that case, see the [sbt website](http://www.scala-sbt.org/) for instructions on installing `sbt`.

If you're using the Activator UI the <a class="shortcut" href="#run">run</a> panel, select one of the bullet items under "Main Class", for example `WordCount2`, and click the "Start" button. The "Logs" panel shows some information. Note the "output" directories listed in the output. Use a file browser to find those directories to view the output written in those locations.

If you are using `sbt` from the command line, start `sbt`, then type `run`. It will present the same list of main classes. Enter one of the numbers to select the executable you want to run. Try **WordCount2** to verify everything works.

Note that the exercises with package names that contain `solns` are solutions to exercises. The `other` exercises show alternative implementations.

Here is a list of the exercises. In subsequent sections, we'll dive into the details for each one. Note that each name ends with a number, indicating the order in which we'll discuss and try them:

* **Intro1:** Actually, this *isn't* listed by the `run` command, because it is a script we'll use with the interactive *Spark Shell*.
* **WordCount2:** The *Word Count* algorithm: Read a corpus of documents, tokenize it into words, and count the occurrences of all the words. A classic, simple algorithm used to learn many Big Data APIs. By default, it uses a file containing the King James Version (KJV) of the Bible. (The `data` directory has a [REAMDE](data/README.html) that discusses the sources of the data files.)
* **WordCount3:** An alternative implementation of *Word Count* that uses a slightly different approach and also uses a library to handle input command-line arguments, demonstrating some idiomatic (but fairly advanced) Scala code.
* **Matrix4:** Demonstrates Spark's Matrix API, useful for many machine learning algorithms.
* **Crawl5a:** Simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines. The documents "crawled" are sample emails from the Enron email dataset, each of which has been classified already as SPAM or HAM.
* **InvertedIndex5b:** Using the crawl data, compute the index of words to documents (emails).
* **NGrams6:** Find all N-word ("NGram") occurrences matching a pattern. In this case, the default is the 4-word phrases in the King James Version of the Bible of the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the NGram Phrase can also be a regular expression, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, and `hated`. 
* **Joins7:** Spark supports SQL-style joins and this exercise provides a simple example.
* **SparkStreaming8:** The streaming capability is relatively new and this exercise shows how it works to construct a simple "echo" server. Running it is a little more involved. See below.

Let's examine these exercises in more detail...

## Intro1:

<a class="shortcut" href="#code/src/main/scala/spark/Intro1.sc">Intro1.sc</a>

Our first exercise demonstrates the useful *Spark Shell*, which is a customized version of Scala's REPL (read, eval, print, loop). We'll copy and paste some commands from the file <a class="shortcut" href="#code/src/main/scala/spark/Intro1.sc">Intro1.sc</a>. 

The comments in this and the subsequent files try to explain the API calls being made.

You'll note that the extension is `.sc`, not `.scala`. This is my convention to prevent the build from compiling this file, which won't compile because it's missing some definitions that the Spark Shell will define automatically.

Actually, we're _not_ going to use the _actual_ Spark Shell, because just pull down the Spark jar file as a dependency, not a full distribution. Instead, we'll just use the Scala REPL via the `sbt console` task. We'll discuss the differences of this approach.

You'll have to use a command window for this part of the workshop. Change your working directory to where you installed this workshop and type `sbt`:

```
cd where_you_installed_the_workshop
sbt
```

At the `sbt` prompt, type `console`. You'll see a welcome message and a `scala>` prompt.

We're going to paste in the code from <a class="shortcut" href="#code/src/main/scala/spark/Intro1.sc">Intro1.sc</a>. You could do it all at once, but we'll do it a few lines at a time and discuss each one. Here is the content of the script without the comments, but broken into sections with discussions:

```
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
val sc = new SparkContext("local", "Intro (1)")
```
Import the `SparkContext` that drives everything.

Why are there two, very similar `import` statements? The first one imports the `SparkContext` type so we don't have to use a fully-qualified name in the `new SparkContext` statement. The second is analogous to a `static import` in Java, where we make some methods and values visible in the current scope, again without requiring qualification.

When you construct a `SparkContext`, there are several constructors you can use. This one takes a string for the "master" and a job name. The master must be one of the following:

* `local`: Start the Spark job standalone and use a single thread to run the job.
* `local[k]`: Use `k` threads instead. Should be less than the number of cores.
* `mesos://host:port`: Connect to a running, Mesos-managed Spark cluster.
* `spark://host:port`: Connect to a running, standalone Spark cluster.

You can also run Spark under Hadoop YARN, which we'll discuss at the end of the workshop.

```
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache
```

Define a read-only variable `input` of type RDD (inferred) by loading the text of the King James Version of the Bible, which has each verse on a line, we then map over the lines converting the text to lower case. 

> The `data` directory has a [README](data/README.html) that discusses the files present and where they came from.

Finally, we cache the data in memory for faster, repeated retrieval. You shouldn't always do this, as it consumes memory, but when your workflow will repeatedly reread the data, caching provides dramatic performance improvements.

```
val sins = input.filter(line => line.contains("sin"))
val count = sins.count()         // How many sins?
val array = sins.collect()       // Convert the RDD into a collection (array)
array.take(20) foreach println   // Take the first 20, and print them 1/line.
```

Filter the input for just those verses that mention "sin" (recall that the text is now lower case). Then count how many found, convert the RDD to a collection. (What is the actual type??) and finally print the first twenty lines.

Note: in Scala, the `()` in method calls are actually optional for no-argument methods.

```
val filterFunc: String => Boolean = 
    (s:String) => s.contains("god") || s.contains("christ") 
```

An alternative approach; create a separate filter _function value_ instead and pass it as an argument to the filter method. Specifically, `filterFunc` is a value that's a function of type `String` to `Boolean`.

Actually, the following more concise form is equivalent, due to type inference:

```
val filterFunc: String => Boolean = 
    s => s.contains("god") || s.contains("christ") 
```

```
val sinsPlusGodOrChrist  = sins filter filterFunc
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
```

Now use `filterFunc` with filter to find all the `sins` verses that mention God or Christ.
Then, count how many were found. Note that we dropped the parentheses after "count" in this case.

```
sc.stop()
```

Stop the session. If you exit the REPL immediately, this will happen implicitly, but as we'll see, it's a good practice to always do this in your scripts. You'll also don't typically do this when running the actual Spark Shell, as you'll usually want the context alive until you're finished completely.

Lastly, we need to run some unrelated code to setup the rest of the exercises. Namely, we need to create an `output` directory we'll use:

```
val output = new java.io.File("output")
if (output.exists == false) output.mkdir
```

Now, if we actually used the Spark Shell for this exercise, we would have omitted the two `import` statements and the statement where we created the `ScalaContext` value `sc`. 
The shell automatically does these steps for us.

There are comments at the end of each source file, including this one, with suggested exercises to learn the API. Try them as time permits. Solutions for some of them are provided in the `src/main/scala/spark/solns` directory. Solutions are provided for all the suggested exercises, but we do accept pull requests ;) All the solutions provided for the rest of the exercises are also built with the rest of the code, so you'll be able to run them the same way.

> Note: If you don't want to modify the original code when working on an exercise, just copy the file and give the exercise type a new name.

Before moving on, let's discuss how you would actually run the Spark Shell. When you [download a full Spark distribution](TODO), it includes a `bin` directory with several Bach shell and Windows scripts. All you need to do from a command window is invoke the command `bin/spark-shell` (assuming your working directory is the root of the distribution).

## WordCount2:

<a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> 

The classic, simple *Word Count* algorithm is easy to understand and it's suitable for parallel computation, so it's a good vehicle when first learning a Big Data API.

In *Word Count*, you read a corpus of documents, tokenize each one into words, and count the occurrences of all the words globally. The initial reading, tokenization, and "local" counts can be done in parallel. 

<a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> uses the same King James Version (KJV) of the Bible file we used in the first exercise. (Subsequent exercises will add the ability to override defaults with command-line arguments.)

If using the <a class="shortcut" href="#run">run</a> panel, select `scala.WordCount2` and click the "Start" button. The "Logs" panel shows some information. Note the "output" directories listed in the output. Use a file browser to find those directories (which have a timestamp) to view the output written in there.

If using `sbt`, enter `run` and then the number shown to the left to `scala.WordCount2`.

The reason the output directories have a timestamp in their name is so you can easily rerun the exercises repeatedly. Starting with v1.0.0, Spark follows the Hadoop convention of refusing to overwrite an existing directory. The timestamps keep them unique.

As before, here is the text of the script in sections, with code comments removed:

```
package spark    // Put the code in a package named "spark"
import spark.util.Timestamp   // Simple date-time utility
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

We use a `spark` package for the compiled exercises. The `Timestamp` class is a simple utility class we implemented to create the timestamps we embed in put output file and directory names.

> Even though our exercises from now on will be compiled classes, you could still use the Spark Shell to try out most constructs. This is especially useful when debugging and experimenting!

First, here is the outline of the script, demonstrating a pattern we'll use throughout.

```
object WordCount2 {
  def main(args: Array[String]) = {

    val sc = new SparkContext("local", "Word Count (2)")

    try {
      ...
    } finally {
      sc.stop()      // Stop (shut down) the context.
    }
  }
}

In case the script fails with an exception, putting the `SparkContext.stop` inside a `finally` clause ensures that will get invoked no matter what. This is most useful when you're running scripts inside the Scala REPL.

The content of the `try` clause is the following:

```
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache

val wc = input
  .flatMap(line => line.split("""\W+"""))
  .map(word => (word, 1))
  .reduceByKey((count1, count2) => count1 + count2)

val now = Timestamp.now()
val out = s"output/kjv-wc2-$now"
println(s"Writing output to: $out")
wc.saveAsTextFile(out)
```

After the same loading and cache of the data we saw previously, we setup a pipeline of operations to perform the word count. 

First the line is split into words using as the separator any run of characters that isn't alphanumeric (e.g., whitespace and punctuation). This also conveniently removes the trailing `~` characters at the end of each line (for some reason). `input.flatMap(line => line.split(...))` maps over each line, expanding it into a collection of words, yielding a collection of collections of words. The `flat` part flattens those nested collections into a single, "flat" collection of words.

The next two lines convert the single word "records" into tuples with the word and a count of `1`. In Shark, the first field in a tuple will be used as the default key for joins, group-bys, and the `reduceByKey` we use next. It effectively groups all the tuples together with the same word (the key) and then "reduces" the values using the passed in function. In this case, the two counts are added together.

The last sequence of statements creates a timestamp that can be used in a file or directory name, constructs the output path, and finally uses the `saveAsTextFile` method to write the final RDD to that location.

Spark follows Hadoop conventions. The `out` path is actually interpreted as a directory name. Here is its contents (for a run at a particular time...):

```
$ ls -Al output/kjv-wc2-2014.05.02-06.40.55
total 328
-rw-r--r--  1 deanwampler  staff       8 May  3 09:40 ._SUCCESS.crc
-rw-r--r--  1 deanwampler  staff    1248 May  3 09:40 .part-00000.crc
-rwxrwxrwx  1 deanwampler  staff       0 May  3 09:40 _SUCCESS
-rwxrwxrwx  1 deanwampler  staff  158620 May  3 09:40 part-00000
```

In a real cluster with lots of data and lots of concurrent processing, there would be many `part-NNNNN` files. They contain the actual data. The `_SUCCESS` file is a useful convention that signals the end of processing. It's useful because tools that are watching for the data to be written so they can perform subsequent processing will know the files are complete when they see this marker file. Finally, there are "hidden" CRC files for these other two files.

There are exercises in the file and solutions for some of them, for example <a class="shortcut" href="#code/src/main/scala/spark/solns/WordCount2GroupBy.scala">solns/WordCount2GroupBy.scala</a> solves a "group by" exercise.

## WordCount3:

<a class="shortcut" href="#code/src/main/scala/spark/WordCount3.scala">WordCount3.scala</a> 

This exercise also implements *Word Count*, but it uses a slightly simpler approach. It also uses a utility library we added to handle input command-line arguments, demonstrating some idiomatic (but fairly advanced) Scala code. 

Finally, it does some data cleansing to improve the results. The sacred text files included in the `data` directory, such as `kjvdat.txt` are actually formatted records of the form:

```
book|chapter|verse|text
```

That is, pipe-separated fields with the book of the Bible (e.g., Genesis, but abbreviated "Gen"), the chapter and verse numbers, and then the verse text. We just want to word count the verses, although including the book names would be fine.

Command line options can be used to override the defaults. You'll have to use `sbt` from a command window to use this feature. Note the use of the `run-main` task that lets us specify a particular "main" to run and optional arguments. The "\" are used to wrap long lines, `[...]` indicate optional arguments, and `|` indicate alternative flags:

```
run-main spark.WordCount3 [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

Where the options have the following meanings:

```
-h | --help     Show help and exit.
-i ... input    Read this input source (default: data/kjvdat.txt).
-o ... output   Write to this output location (default: output/kjvdat-wc3).
-m ... master   local, local[k], etc. as discussed previously.
```

You can try different variants of `local[k]`, but keep k less than the number of cores in your machine. The `input` and `master` arguments are basically the same things we discussed for `WordCount2`, but the `output` argument is used slightly differently. As we'll see, we'll output the results using a different mechanism than before, so the `output/kjvdat-wc3` (or your override) will be converted to file (not a Hadoop-style directory) `output/kjvdat-wc3-${now}.txt`, where `now` will be the current timestamp.

When you specify an input path for Spark, you can specify `bash`-style "globs" and even a list of them.

* `data/foo`: Just the file `foo` or if it's a directory, all its files, one level deep (unless the program does some extra handling itself).
* `data/foo*.txt`: All files in `data` whose names start with `foo` and end with the `.txt` extension.
* `data/foo*.txt,data2/bar*.dat`: A comma-separated list of globs.

Here is the implementation of `WordCount3`, in sections:

```
package spark
import spark.util.{CommandLineOptions, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

As before, but with our new `CommandLineOptions` and `Timestamp` utilities.

```
object WordCount3 {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-wc3"),
      CommandLineOptions.master("local"))

    val argz = options(args.toList)
```

I won't discuss the implementation of <a class="shortcut" href="#code/src/main/scala/spark/util/CommandLineOptions.scala">CommandLineOptions.scala</a> except to say that it defines some methods that create instances of an `Opt` type, one for each of the options we discussed above. The single argument to this method is the default value. 

```
    val sc = new SparkContext(argz("master").toString, "Word Count (3)")

    try {
      val input = sc.textFile(argz("input-path").toString)
        .map(line => line.toLowerCase.split("\\s*\\|\\s*").last)
      input.cache
```

It starts out much like `WordCount2`, but it splits each line into fields, where the lines are of the form: `book|chapter|verse|text`. Only the text is kept. The output is an RDD that we then cache as before. Note that calling `last` on the split array is robust against lines that don't have the delimiter, if there are any; it simply returns the whole original string.

```
      val wc2 = input
        .flatMap(line => line.split("""\W+"""))
        .countByValue()  // Returns a Map[T, Long]
```

Split on non-alphanumeric sequences of character as before, but rather than map to `(word, 1)` tuples and use `reduceByKey`, as we did in `WordCount2`, we simply treat the words as values and call `countByValue` to count the unique occurrences.

```
      val now = Timestamp.now()
      val outpath = s"${argz("output-path")}-$now"
      println(s"Writing output (${wc2.size} records) to: $outpath")
      import java.io._
      val out = new PrintWriter(outpath)
      wc2 foreach {
        case (word, count) => out.println("%20s\t%d".format(word, count))
      }
      // WARNING: Without this close statement, it appears the output stream is 
      // not completely flushed to disk!
      out.close()
    } finally {
      sc.stop()
    }
  }
}
```

The result of `countByValue` is a Scala array, not an RDD, so we use conventional Java I/O to write the output. Note the warning; failure to close the stream explicit appears to cause data truncation when the `SparkContext` is stopped before the stream is flushed.

Don't forget the try the exercises at the end of the source file. 

## Matrix4

<a class="shortcut" href="#code/src/main/scala/spark/Matrix4.scala">Matrix4.scala</a> 

Spark was originally used for Machine Learning algorithms. It has a built-in Matrix API that is useful for many machine learning algorithms. This exercise explores it briefly.

The sample data is generated internally; there is no input that is read. The output is written to the console.

Here is the `run-main` command with options:

```
run-main spark.Matrix4 [m [n]]
```

Where the smaller set of supported options are:

```
m   Number of rows (default: 5)
n   Number of columns (default: 10)
```

Here is the code:

```
package spark
import spark.util.{Matrix, Timestamp}
import org.apache.spark.SparkContext

object Matrix4 {

  def main(args: Array[String]) = {

    case class Dimensions(m: Int, n: Int)

    val dims = args match {
      case Array(m, n) => Dimensions(m.toInt, n.toInt)
      case Array(m)    => Dimensions(m.toInt, 10)
      case Array()     => Dimensions(5,       10)
      case _ => 
        println("""Expected optional matrix dimensions, got this: ${args.mkString(" ")}""")
        sys.exit(1)
    }
```

`Dimensions` is a convenience class for capturing the default or user-specified matrix dimensions.

```
    val sc = new SparkContext("local", "Matrix (4)")

    try { 
      // Set up a mxn matrix of numbers.
      val matrix = Matrix(dims.m, dims.n)

      // Average rows of the matrix in parallel:
      val sums_avgs = sc.parallelize(1 to dims.m).map { i =>
        // Matrix indices count from 0. 
        // "_ + _" is the same as "(count1, count2) => count1 + count2".
        val sum = matrix(i-1) reduce (_ + _) 
        (sum, sum/dims.n)
      }.collect

      println(s"${dims.m}x${dims.n} Matrix:")
      sums_avgs.zipWithIndex foreach {
        case ((sum, avg), index) => 
          println(f"Row #${index}%2d: Sum = ${sum}%4d, Avg = ${avg}%3d")
      }
    } finally {
      sc.stop()
    }
  }
}
```

The comments explain most of the steps. The crucial part is the call to `parallelize` that creates N parallel operations. If you have less than N cores, some of the operations will have to run sequentially. The argument to `parallelize` is a sequence of "things" where each one will be passed to one of the operations. Here, we just use the literal syntax to construct a sequence of integers from 1 to the number of rows. When the anonymous function is called, one of those row numbers will get assigned to `i`. We then grab the `i-1` row (because of zero indexing) and use the `reduce` method to sum the column elements. A final tuple with the sum and the average is returned. 

The `collect` method is called to convert the RDD to an array, because we're just going to print results to the console. The expression `sums_avgs.zipWithIndex` creates a tuple with each `sumb_avgs` value and it's index into the collection. We use that to print the row index.

## Crawl5a

<a class="shortcut" href="#code/src/main/scala/spark/Crawl5a.scala">Crawl5a.scala</a> 

This the first part of the fifth exercise. It simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines, from words to documents. The documents "crawled" are sample emails from the Enron email dataset, each of which has been previously classified already as SPAM or HAM.

`Crawl5a` supports the same command-line options as `WordCount3`:

```
run-main spark.Crawl5a [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

In this case, no timestamp is appended to the output path, since it will be read by the next exercise `InvertedIndex5b`. So, if you rerun `Crawl5a`, you'll have to delete or rename the previous output manually.

Most of this code is straightforward. It's comments explain any complicated constructs used. 

## InvertedIndex5b

<a class="shortcut" href="#code/src/main/scala/spark/InvertedIndex5b.scala">InvertedIndex5b.scala</a> 

Using the crawl data just generated, compute the index of words to documents (emails).

`InvertedIndex5b` supports the same command-line options as `WordCount3`:

```
run-main spark.InvertedIndex5b [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

Here is the code:

```
package spark

import spark.util.{CommandLineOptions, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object InvertedIndex5b {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("output/crawl"),
      CommandLineOptions.outputPath("output/inverted-index"),
      CommandLineOptions.master("local"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "Inverted Index (5b)")

    try {
      val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
      val input = sc.textFile(argz("input-path").toString) map {
        case lineRE(name, text) => (name.trim, text.toLowerCase)
        case badLine => 
          Console.err.println("Unexpected line: $badLine")
          ("", "")  
      }
```

Inside the `try` expression, we load the "crawl" data, where each line was written by `Crawl5a` with the following format: `(document_id, text)` (including the parentheses). Hence, we use a regular expression with "capture groups" for the document and text.

Note the function passed to `map`. It has the form:

```
{
  case lineRE(name, text) => ...
  case line => ...
}
```

There is now explicit argument list like we've used before. This syntax is the literal syntax for a *partial function*, a mathematical concept for a function that is not defined at all of its inputs. We have two `case` match clauses, one for when the regular expression successfully matches and returns the capture groups into variables `name` and `text` and the second which will match everything else, assigning the line to the variable `badLine`. (In fact, this catch-all clause makes the function *total*, not *partial*.) The function must return a two-element tuple, so the catch clause simply returns `("","")`.

Note that the specified or default `input-path` is a directory with Hadoop-style content, as discussed previously. Spark knows to ignore the "hidden" files.

```
      val now = Timestamp.now()
      val out = s"${argz("output-path")}-$now"
      println(s"Writing output to: $out")

      // Split on non-alphanumeric sequences of character as before. 
      // Rather than map to "(word, 1)" tuples, we treat the words by values
      // and count the unique occurrences.
      input
        .flatMap { 
          case (path, text) => text.split("""\W+""") map (word => (word, path))
        }
        .map { 
          case (word, path) => ((word, path), 1) 
        }
        .reduceByKey{
          case (count1, count2) => count1 + count2
        }
        .map {
          case ((word, path), n) => (word, (path, n)) 
        }
        .groupBy {
          case (word, (path, n)) => word
        }
        .map {
          case (word, seq) => 
            val seq2 = seq map {
              case (redundantWord, (path, n)) => (path, n)
            }
            (word, seq2.mkString(", "))
        }
        .saveAsTextFile(out)
    } finally {
      sc.stop()
    }
  }
}
```

See if you can understand what this sequence of transformations is doing. The end goal is to output each record string in the following form: `(word, (doc1, n1), (doc2, n2), ...)`:

## NGrams6

<a class="shortcut" href="#code/src/main/scala/spark/NGrams6.scala">NGrams6.scala</a> 

In *Natural Language Processing*, one goal is to determine the sentiment or meaning of text. One technique that helps do this is to locate the most frequently-occurring, N-word phrases, or *NGrams*. Longer NGrams can convey more meaning, but they occur less frequently so all of them appear important. Shorter NGrams have better statistics, but each one conveys less meaning. In most cases, N=3-5 appears to provide the optimal balance.

This exercise finds all NGrams matching a user-specified pattern. The default is the 4-word phrases the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the user can also specify an NGram Phrase that is a regular expression or a mixture, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, or `hated` as the second word. 

`NGrams6` supports the same command-line options as `WordCount3`, except for the output path (it just writes to the console), plus two new options:

```
run-main spark.NGrams6 [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-m | --master master] \ 
  [-c | --count N] \ 
  [-n | --ngrams string] 
```

Where

```
-c | --count N        List the N most frequently occurring NGrams (default: 100)
-n | --ngrams string  Match string (default "% love % %"). Quote the string!
```
                      
The `%` are wildcards for words and the whitespace is replaced with a more general regular expression. You can specify regular expressions if you want. What would the following match?

```
-n "% (lov|hat)ed? % %"
```

Here r yur codez:

```
package spark

import spark.util.{CommandLineOptions, Timestamp}
import spark.util.CommandLineOptions.Opt
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object NGrams6 {
  def main(args: Array[String]) = {

    /** A function to generate an Opt for handling the count argument. */
    def count(value: String): Opt = Opt(
      name   = "count",
      value  = value,
      help   = s"-c | --count  N  The number of NGrams to compute (default: $value)",
      parser = {
        case ("-c" | "--count") +: n +: tail => (("count", n), tail)
      })
    
    /** A function to generate an Opt for handling the ngram expression. */
    def ngrams(value: String): Opt = Opt(
      name   = "ngrams",
      value  = value,
      help   = s"-n | --ngrams  S     The NGrams match string (default: $value)",
      parser = {
        case ("-n" | "--ngrams") +: s +: tail => (("ngrams", s), tail)
      })
    
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.master("local"),
      count("100"),
      ngrams("% love % %"))

    val argz = options(args.toList)

    val sc = new SparkContext(argz("master").toString, "NGrams (6)")
    val ngramsStr = argz("ngrams").toString.toLowerCase
    // Note that the replacement strings use Scala's triple quotes; necessary
    // to ensure that the final string is "\w+" and "\s+" for the reges.
    val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
    val n = argz("count").toInt
```

Without discussing the details, this exercise defines two new `Opt` instances for handling the NGram phrase and count options. Because we allow the user to mix `%` characters for word wildcards and regular expressions, we convert the `%` to regexs for words and also replace all runs of whitespace with a more flexible regex for whitespace.

``` 
    try {
      object CountOrdering extends Ordering[(String,Int)] {
        def compare(a:(String,Int), b:(String,Int)) = 
          -(a._2 compare b._2)  // - so that it sorts descending
      }

      val ngramz = sc.textFile(argz("input-path").toString)
        .flatMap { line => 
            val text = line.toLowerCase.split("\\s*\\|\\s*").last
            ngramsRE.findAllMatchIn(text).map(_.toString)
        }
        .map(ngram => (ngram, 1))
        .reduceByKey((count1, count2) => count1 + count2)
        .takeOrdered(n)(CountOrdering)

      println(s"Found ${ngramz.size} ngrams:")
      ngramz foreach {
        case (ngram, count) => println("%30s\t%d".format(ngram, count))
      }
    } finally {
      sc.stop()
    }
  }
}
```

We need an implementation of `Ordering` to sort our found NGrams descending by count.
We read the data as before, but note that because of our line orientation, we *won't* find NGrams that cross line boundaries! This doesn't matter for our sacred text files, since it wouldn't make sense to find NGrams across verse boundaries, but a more flexible implementation should account for this. Note that we also look at just the verse text, as in `WordCount3`.

The `map` and `reduceByKey` calls are just like we used previously for `WordCount2`, but now we're counting found NGrams. The final `takeOrdered` call combines sorting with taking the top `n` found. This is more efficient than separate sort, then take operations. As a rule, when you see a method that does two things like this, it's usually there for efficiency reasons!

## SparkStreaming8

<a class="shortcut" href="#code/src/main/scala/spark/SparkStreaming8.scala">SparkStreaming8.scala</a> 

The streaming capability is relatively new and our last exercise shows how it works to construct a simple "echo" server. Running it is a little more involved, because we need two console windows, or at least one in addition to `sbt run`.

You'll need [NetCat](http://netcat.sourceforge.net/) or NCat that comes with [NMap](http://nmap.org/download.html), which is available for more platforms, like Windows. If you're on a Mac or Linux machine, you may already have the `nc` (NetCat) shell command available. 

We'll use it to send data to the spark streaming process. Open a second console window to use `nc` or `ncat`.

In Activator or your `sbt` console window, run this command in `SparkStreaming8` as before. Note that it has default arguments for the host (`localhost`) and the port (`9999`), e.g., you could use this `sbt` command to override them:

```
run-main spark.SparkStreaming8 some_host some_port
```

However you start it, it will wait for traffic on the socket.

In the second console, run this command or the equivalent for `ncat`:

```
nc -c -l -p 9999
```

The `-c` option tells it to terminate if the socket drops, the `-l` option puts `nc` in listen mode, and the `-p` option is used to specify the port.

Now, type (or copy and paste) text into the console running `nc`. After each carriage return, the text is sent to the `SparkStreaming8` app, where word count is performed on it.

Unfortunately, Spark Streaming does not yet provide a way to detect the end of
input from the socket! (A feature request has been posted.) So, we can't just end the `nc` process and have the `SparkStreaming8` app exit gracefully. Instead, we have to ^C to kill `SparkStreaming8` (and `sbt` with it). Because we invoked `nc` with `-c` it will terminate automatically when we do this; `nc` **does** detect dropped sockets.

Spark Streaming uses a clever hack; it runs more or less the same Spark code (or code that at least looks conceptually the same) on deltas of data, say all the events received within 1-second intervals. Those deltas are `RDDs` encapsulated in a `DStream` (Discretized Stream).

Here is the code for <a class="shortcut" href="#code/src/main/scala/spark/SparkStreaming8.scala">SparkStreaming8.scala</a>:

```
object SparkStreaming8 {
  def main(args: Array[String]) = {
    val conf = new SparkConf()
             .setMaster("local[2]")
             .setAppName("Spark Streaming (8)")
             .set("spark.cleaner.ttl", "60")
             .set("spark.files.overwrite", "true")
             // If you need more memory:
             // .set("spark.executor.memory", "1g")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
```

Here we construct the `SparkContext` a different way, by first defining a `SparkConf` (configuration) object. First, it is necessary to use 2 cores, which is specified using `setMaster("local[2]")` to avoid a [problem discussed here](http://apache-spark-user-list.1001560.n3.nabble.com/streaming-questions-td3281.html).

Spark Streaming requires the TTL to be set, `spark.cleaner.ttl`, which defaults to infinite. This specifies the duration in seconds for how long Spark should remember any metadata, such as the stages and tasks generated, etc. Periodic cleanups is necessary for long-running streaming jobs. Note that an RDD that persists in memory for more than this duration will be cleared as well. See [Configuration](http://spark.apache.org/docs/0.9.0/configuration.html) for more details.

With the `SparkContext`, we create a `StreamingContext`, where we also specify the time interval.

```
    val (server, port) = args.toList match {
      case server :: port :: _ => (server, port.toInt)
      case port :: Nil => ("localhost", port.toInt)
      case Nil => ("localhost", 9999)
    }
    println(s"Connecting to $server:$port...")
```

Use a simple handler for the command-line argument list to extract the optional hostname and port.

```
    val lines = ssc.socketTextStream(server, port)
```

Create a `DStream` (Discretized Stream) named `lines` that will connect to the specified server and port. It will periodically generate an RDD from a discrete chunk of the data.

Now we implement an incremental word count:

```
    val words = lines.flatMap(line => line.split("""\W+"""))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.transform(rdd => rdd.reduceByKey(_ + _))

    wordCounts.print()  // print a few counts...

    val now = Timestamp.now()
    val out = s"output/streaming/kjv-wc-$now"
    println(s"Writing output to: $out")

    wordCounts.saveAsTextFiles(out, "txt")

    ssc.start()
    ssc.awaitTermination()
  }
}
```

This works much like our previous word count logic, except for the use of `transform`, a `DStream` method for transforming the `RDDs` into new `RDDs`. In this case, we are performing "mini-word counts", within each RDD, but not across the whole `DStream`.

## Best Practices, Tips, and Tricks

### Safe Closures

When you use a closure (anonymous function), Spark will serialize it and send it around the cluster. This means that any captured variables must be serializable. 

A common mistake is to capture a field in an object, which forces the whole object to be serialized. Sometimes it can't be. Consider this example adapted from [this presentation](http://spark-summit.org/wp-content/uploads/2013/10/McDonough-spark-tutorial_spark-summit-2013.pdf).

```
class RDDApp {
  val factor = 3.14159
  val log = new Log(...)

  def multiply(rdd: RDD[Int]) = {
    rdd.map(x => x * factor).reduce(...)
  }
}
```

The closure passed to `map` captures the field `factor` in the instance of `RDDApp`. However, the JVM must serialize the whole object, and a `NotSerializableException` will result when it attempts to serialize `log`. 

Here is the work around; assign `factor` to a local field:

```
class RDDApp {
  val factor = 3.14159
  val log = new Log(...)

  def multiply(rdd: RDD[Int]) = {
    val factor2 = factor
    rdd.map(x => x * factor2).reduce(...)
  }
}
```

Now, only `factor2` must be serialized.

## Going Forward from Here

This template is not a complete Apache Spark tutorial. To learn more, see the following:

* The Apache Spark [website](http://spark.apache.org/). 
* The Apache Spark [tutorial](http://spark.apache.org/tree/develop/tutorial) distributed with the [Apache Spark](http://spark.apache.org) distribution. See also the examples in the distribution and be sure to study the [Scaladoc](http://spark.apache.org/docs/0.9.0/api.html) pages for key types such as `RDD` and `SchemaRDD`.
* [Talks from Spark Summit 2013](http://spark-summit.org/2013).
* [Running Spark in EC2](http://aws.amazon.com/articles/4926593393724923).
* [Running Spark on Mesos](http://mesosphere.io/learn/run-spark-on-mesos/).

**Experience Reports:**

* [Spark at Twitter](http://www.slideshare.net/krishflix/seattle-spark-meetup-spark-at-twitter)

**Other Spark Based Libraries:**

* [Snowplow's Spark Example Project](https://github.com/snowplow/spark-example-project).
* [Thunder - Large-scale neural data analysis with Spark](https://github.com/freeman-lab/thunder).

## For more about Typesafe:

* See [Typesafe Activator](http://typesafe.com/activator) to find other Activator templates.
* See [Typesafe](http://typesafe.com) for more information about our products and services. 

