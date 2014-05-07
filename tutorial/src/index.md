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

Spark also includes [EC2 launch scripts](http://spark.apache.org/docs/0.9.1/ec2-scripts.html) for running clusters on Amazon EC2.

## Resilient, Distributed Datasets

The data caching is one of the key reasons that Spark's performance is considerably better than the performance of MapReduce. Spark stores the data for the job in *Resilient, Distributed Datasets* (RDDs), where a logical data set is virtualized over the cluster. 

The user can specify that data in an RDD should be cached in memory for subsequent reuse. In contrast, MapReduce has no such mechanism, so a complex job requiring a sequence of MapReduce jobs will be penalized by a complete flush to disk of intermediate data, followed by a subsequent reloading into memory by the next job.

RDDs support common data operations, such as *map*, *flatmap*, *filter*, *fold/reduce*, and *groupby*. RDDs are resilient in the sense that if a "partition" of data is lost on one node, it can be reconstructed from the original source without having to start the whole job over again.

The architecture of RDDs is described in the research paper [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf).

## Spark 1.0.0

We will use Spark 1.0.0-RC3 so we can use some new features, like Spark SQL, that will be generally available (GA) soon. I'll update the workshop to final artifacts when they are available.

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
* **SparkSQL9:** An "alpha" feature of Spark 1.0.0 is an integrated SQL query library that is based on a new query planner called Catalyst. The plan is to replace the Shark (Hive) query planner with Catalyst. For now, SparkSQL provides an API for running SQL queries over RDDs and seamless interoperation with Hive/Shark tables.
* **Shark10:** We'll briefly look at Shark, a port of the MapReduce-based [Hive](http://hive.apache.org) SQL query tool to Spark.
* **MLlib11:** One of the early uses for Spark was machine learning (ML) applications. It's not an extensive library, but it's growing fast. For example, the [Mahout](http://mahout.apache.org) project is planning to port its MapReduce algorithsm to Spark. This exercise looks at a representative ML problem.
* **GraphX12:** Our last exercise explores the Spark graph library GraphX.

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

```scala
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

```scala
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache
```

Define a read-only variable `input` of type RDD (inferred) by loading the text of the King James Version of the Bible, which has each verse on a line, we then map over the lines converting the text to lower case. 

> The `data` directory has a [README](data/README.html) that discusses the files present and where they came from.

Finally, we cache the data in memory for faster, repeated retrieval. You shouldn't always do this, as it consumes memory, but when your workflow will repeatedly reread the data, caching provides dramatic performance improvements.

```scala
val sins = input.filter(line => line.contains("sin"))
val count = sins.count()         // How many sins?
val array = sins.collect()       // Convert the RDD into a collection (array)
array.take(20) foreach println   // Take the first 20, and print them 1/line.
```

Filter the input for just those verses that mention "sin" (recall that the text is now lower case). Then count how many found, convert the RDD to a collection. (What is the actual type??) and finally print the first twenty lines.

Note: in Scala, the `()` in method calls are actually optional for no-argument methods.

```scala
val filterFunc: String => Boolean = 
    (s:String) => s.contains("god") || s.contains("christ") 
```

An alternative approach; create a separate filter _function value_ instead and pass it as an argument to the filter method. Specifically, `filterFunc` is a value that's a function of type `String` to `Boolean`.

Actually, the following more concise form is equivalent, due to type inference:

```scala
val filterFunc: String => Boolean = 
    s => s.contains("god") || s.contains("christ") 
```

```scala
val sinsPlusGodOrChrist  = sins filter filterFunc
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
```

Now use `filterFunc` with filter to find all the `sins` verses that mention God or Christ.
Then, count how many were found. Note that we dropped the parentheses after "count" in this case.

```scala
sc.stop()
```

Stop the session. If you exit the REPL immediately, this will happen implicitly, but as we'll see, it's a good practice to always do this in your scripts. You'll also don't typically do this when running the actual Spark Shell, as you'll usually want the context alive until you're finished completely.

Lastly, we need to run some unrelated code to setup the rest of the exercises. Namely, we need to create an `output` directory we'll use:

```scala
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

```scala
package spark    // Put the code in a package named "spark"
import spark.util.Timestamp   // Simple date-time utility
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

We use a `spark` package for the compiled exercises. The `Timestamp` class is a simple utility class we implemented to create the timestamps we embed in put output file and directory names.

> Even though our exercises from now on will be compiled classes, you could still use the Spark Shell to try out most constructs. This is especially useful when debugging and experimenting!

First, here is the outline of the script, demonstrating a pattern we'll use throughout.

```scala
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

```scala
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

```scala
package spark
import spark.util.{CommandLineOptions, Timestamp}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

As before, but with our new `CommandLineOptions` and `Timestamp` utilities.

```scala
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

```scala
    val sc = new SparkContext(argz("master").toString, "Word Count (3)")

    try {
      val input = sc.textFile(argz("input-path").toString)
        .map(line => line.toLowerCase.split("\\s*\\|\\s*").last)
      input.cache
```

It starts out much like `WordCount2`, but it splits each line into fields, where the lines are of the form: `book|chapter|verse|text`. Only the text is kept. The output is an RDD that we then cache as before. Note that calling `last` on the split array is robust against lines that don't have the delimiter, if there are any; it simply returns the whole original string.

```scala
      val wc2 = input
        .flatMap(line => line.split("""\W+"""))
        .countByValue()  // Returns a Map[T, Long]
```

Split on non-alphanumeric sequences of character as before, but rather than map to `(word, 1)` tuples and use `reduceByKey`, as we did in `WordCount2`, we simply treat the words as values and call `countByValue` to count the unique occurrences.

```scala
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


## Crawl5a

<a class="shortcut" href="#code/src/main/scala/spark/Crawl5a.scala">Crawl5a.scala</a> 

The first part of the fifth exercise. Simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines. The documents "crawled" are sample emails from the Enron email dataset, each of which has been previously classified already as SPAM or HAM.

`Crawl5a` supports the same command-line options as `WordCount3`:

```
run-main spark.Crawl5a [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

In this case, no timestamp is append to the output path, since it will be read by the next exercise `InvertedIndex5b`. So, if you rerun `Crawl5a`, you'll have to delete or rename the previous output.

## InvertedIndex5b

<a class="shortcut" href="#code/src/main/scala/spark/InvertedIndex5b.scala">InvertedIndex5b.scala</a> 

Using the crawl data, compute the index of words to documents (emails).

`Crawl5a` supports the same command-line options as `WordCount3`:

```
run-main spark.InvertedIndex5b [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

## NGrams6

<a class="shortcut" href="#code/src/main/scala/spark/NGrams6.scala">NGrams6.scala</a> 

Find all N-word ("NGram") occurrences matching a pattern. In this case, the default is the 4-word phrases in the King James Version of the Bible of the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the NGram Phrase can also be a regular expression, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, and `hated`. 

`NGrams6` supports the same command-line options as `WordCount3`, except for the output path (it just writes to the console), plus two more:

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

## SparkStreaming7

<a class="shortcut" href="#code/src/main/scala/spark/SparkStreaming7.scala">SparkStreaming7.scala</a> 

The streaming capability is relatively new and this exercise shows how it works to construct a simple "echo" server. Running it is a little more involved.

## SparkSQL8

<a class="shortcut" href="#code/src/main/scala/spark/SparkSQL8.scala">SparkSQL8.scala</a> 

An "alpha" feature of Spark 1.0.0 is an integrated SQL query library that is based on a new query planner called Catalyst. The plan is to replace the Shark (Hive) query planner with Catalyst. For now, SparkSQL provides an API for running SQL queries over RDDs and seamless interoperation with Hive/Shark tables.

`SparkSQL8` supports the same command-line options as `WordCount3`:

```
run-main spark.SparkSQL8 [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

## Shark9

<a class="shortcut" href="#code/src/main/scala/spark/Shark9.hql">Shark9.hql</a> 

We'll briefly look at Shark, a port of the MapReduce-based [Hive](http://hive.apache.org) SQL query tool to Spark.

## MLlib10

<a class="shortcut" href="#code/src/main/scala/spark/MLlib10.scala">MLlib10.scala</a> 

One of the early uses for Spark was machine learning (ML) applications. It's not an extensive library, but it's growing fast. For example, the [Mahout](http://mahout.apache.org) project is planning to port its MapReduce algorithsm to Spark. This exercise looks at a representative ML problem.

## GraphX11

<a class="shortcut" href="#code/src/main/scala/spark/GraphX11.scala">GraphX11.scala</a> 

(Not to be confused with the X11 windowing system...) Our last exercise explores the Spark graph library GraphX.


## Running on Hadoop

After testing your scripts, you can run them on a Hadoop cluster. You'll first need to build an all-inclusive jar file that contains all the dependencies, including the Scala standard library, that aren't already on the cluster.

The `activator assembly` command first runs an `update` task, if missing dependencies need to be downloaded. Then the task builds the all-inclusive jar file, which is written to `target/scala-2.10/activator-scalding-X.Y.Z.jar`, where `X.Y.Z` will be the current version number for this project.

One the jar is built and assuming you have the `hadoop` command installed on your system (or the server to which you copy the jar file...), the following command syntax will run one of the scripts

```
hadoop jar target/scala-2.10/activator-scalding-X.Y.Z.jar SCRIPT_NAME \ 
  [--hdfs | --local ] [--host JOBTRACKER_HOST] \ 
  --input INPUT_PATH --output OUTPUT_PATH \ 
  [other-args] 
```

Here is an exercise for `NGrams`, using HDFS, not the local file system, and assuming the JobTracker host is determined from the local configuration files, so we don't have to specify it:

```
hadoop jar target/scala-2.10/activator-scalding-X.Y.Z.jar NGrams \ 
  --hdfs  --input /data/docs --output output/wordcount \ 
  --count 100 --ngrams "% loves? %"
```

Note that when using HDFS, Hadoop treats all paths as *directories*. So, all the files in an `--input` directory will be read. In `--local` mode, the paths are interpreted as *files*.

An alternative to running the `hadoop` command directly is to use the `scald.rb` script that comes with Apache Spark distributions. See the [Apache Spark](http://spark.apache.org) website for more information.

## Best Practices, Tips, and Tricks

### Safe Closures

When you use a closure (anonymous function), Spark will serialize it and send it around the cluster. This means that any captured variables must be serializable. 

A common mistake is to capture a field in an object, which forces the whole object to be serialized. Sometimes it can't be. Consider this example adapted from [this presentation](http://spark-summit.org/wp-content/uploads/2013/10/McDonough-spark-tutorial_spark-summit-2013.pdf).

```scala
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

```scala
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

## Other Features of the API

TODO

## YARN

TODO

## Going Forward from Here

This template is not a complete Apache Spark tutorial. To learn more, see the following:

* The Apache Spark [website](http://spark.apache.org/). 
* The Apache Spark [tutorial](http://spark.apache.org/tree/develop/tutorial) distributed with the [Apache Spark](http://spark.apache.org) distribution. See also the examples in the distribution.
* [Talks from Spark Summit 2013](http://spark-summit.org/2013).
* [Running Spark in EC2](http://aws.amazon.com/articles/4926593393724923).

## Experience Reports

* [Spark at Twitter](http://www.slideshare.net/krishflix/seattle-spark-meetup-spark-at-twitter)

## Spark Based Libraries

* [Snowplow's Spark Example Project](https://github.com/snowplow/spark-example-project).
* [Thunder - Large-scale neural data analysis with Spark](https://github.com/freeman-lab/thunder).

## For more about Typesafe:

* See [Typesafe Activator](http://typesafe.com/activator) to find other Activator templates.
* See [Typesafe](http://typesafe.com) for more information about our products and services. 

