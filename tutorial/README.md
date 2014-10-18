## Apache Spark Workshop

![](http://spark.apache.org/docs/latest/img/spark-logo-100x40px.png)

Dean Wampler, Ph.D.
[Typesafe](http://typesafe.com)
[dean.wampler@typesafe.com](mailto:dean.wampler@typesafe.com)
[@deanwampler](https://twitter.com/deanwampler)

This workshop demonstrates how to write and run [Apache Spark](http://spark.apache.org) *Big Data* applications. You can run the examples and exercises locally on a workstation, on Hadoop (which could also be on your workstation), or both.

If you are most interested in using Spark with Hadoop, the Hadoop vendors have preconfigured, virtual machine "sandboxes" with Spark included. See their websites for information.

For more advanced Spark training and services from Typesafe, please visit [typesafe.com/reactive-big-data](http://www.typesafe.com/platform/reactive-big-data/spark).

## Introduction: What Is Spark?

Let's start with an overview of Spark, then discuss how to setup and use this workshop.

[Apache Spark](http://spark.apache.org) is a distributed computing system written in Scala for distributed data programming.

Spark includes support for event stream processing, as well as more traditional batch-mode applications. There is a [SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) module for working with data sets through SQL queries. It integrates the core Spark API with embedded SQL queries with defined schemas. It also offers [Hive](http://hive.apache.org) integration so you can query existing Hive tables, even create and delete them. Finally, it has JSON support, where records written in JSON can be parsed automatically with the schema inferred and RDDs can be written as JSON.

There is also an interactive shell, which is an enhanced version of the Scala REPL (read, eval, print loop shell). SparkSQL adds a SQL-only REPL shell. For completeness, you can also use a custom Python shell that exposes Spark's Python API. A Java API is also supported and R support is under development.

## Why Spark?

By 2013, it became increasingly clear that a successor was needed for the venerable [Hadoop MapReduce](http://wiki.apache.org/hadoop/MapReduce) compute engine. MapReduce applications are difficult to write, but more importantly, MapReduce has significant performance limitations and it can't support event-streaming ("real-time") scenarios.

Spark was seen as the best, general-purpose alternative, so all the major Hadoop vendors announced support for it in their distributions.

## Spark Clusters

Let's briefly discuss the anatomy of a Spark cluster, adapting [this discussion (and diagram) from the Spark documentation](http://spark.apache.org/docs/latest/cluster-overview.html). Consider the following diagram:

![](http://spark.apache.org/docs/latest/img/cluster-overview.png)

Each program we'll write is a *Driver Program*. It uses a [SparkContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext) to communicate with the *Cluster Manager*, which is an abstraction over [Hadoop YARN](http://hortonworks.com/hadoop/yarn/), local mode, standalone (static cluster) mode, Mesos, and EC2.

The *Cluster Manager* allocates resources. An *Executor* JVM process is created on each worker node per client application. It manages local resources, such as the cache (see below) and it runs tasks, which are provided by your program in the form of Java jar files or Python scripts.

Because each application has its own executor process per node, applications can't share data through the *Spark Context*. External storage has to be used (e.g., the file system, a database, a message queue, etc.)

## Resilient, Distributed Datasets

The data caching is one of the key reasons that Spark's performance is considerably better than the performance of MapReduce. Spark stores the data for the job in *Resilient, Distributed Datasets* (RDDs), where a logical data set is virtualized over the cluster.

The user can specify that data in an RDD should be cached in memory for subsequent reuse. In contrast, MapReduce has no such mechanism, so a complex job requiring a sequence of MapReduce jobs will be penalized by a complete flush to disk of intermediate data, followed by a subsequent reloading into memory by the next job.

RDDs support common data operations, such as *map*, *flatmap*, *filter*, *fold/reduce*, and *groupby*. RDDs are resilient in the sense that if a "partition" of data is lost on one node, it can be reconstructed from the original source without having to start the whole job over again.

The architecture of RDDs is described in the research paper [Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf).

## SparkSQL

[SparkSQL](http://spark.apache.org/docs/latest/sql-programming-guide.html) adds the ability to specify schema for RDDs, run SQL queries on them, and even create, delete, and query tables in [Hive](http://hive.apache.org), the original SQL tool for Hadoop. Recently, support was added for parsing JSON records, inferring their schema, and writing RDDs in JSON format.

Several years ago, the Spark team ported the Hive query engine to Spark, calling it [Shark](http://shark.cs.berkeley.edu/). That port is now deprecated. SparkSQL will replace it once it is feature compatible with Hive. The new query planner is called [Catalyst](http://databricks.com/blog/2014/03/26/spark-sql-manipulating-structured-data-using-spark-2.html).

## The Spark Version

This workshop uses Spark 1.1.0.

The following documentation links provide more information about Spark:

* [Documentation](http://spark.apache.org/docs/latest/).
* [Scaladocs API](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package).

The [Documentation](http://spark.apache.org/docs/latest/) includes a getting-started guide and overviews. You'll find the [Scaladocs API](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package) useful for the workshop.

## Setup for the Workshop

You can work through the examples and exercises on a local workstation. I'll refer to this arrangement as *local mode*. If you have Hadoop installation available, including a virtual machine "sandbox" from one of the Hadoop vendors, you can also run most of the examples in that environment. I'll refer to this arrangement as *Hadoop mode*.

Let's discuss setup for local mode first.

## Setup for Local Mode

Working in *local mode* makes it easy to edit, test, run, and debug applications quickly. Then, running them in Hadoop provides more real-world testing.

We will build the examples and exercises using [SBT](http://www.scala-sbt.org/) or [Typesafe Activator](http://typesafe.com/activator). SBT is the de facto standard build tool for Scala. It is a command-line tool.

[Activator](https://typesafe.com/activator) is part of the [Typesafe Reactive Platform](https://typesafe.com/platform/getstarted). It is a web-based environment for finding and using example templates for many different JVM-based toolkits and example applications. Once you've loaded one or more templates, you can browse and build the code, then run the tests and the application itself. This *Spark Workshop* is one example.

Activator also includes SBT, which the UI uses under the hood. You can use the *shell* mode explicitly if you prefer running `sbt` "tasks".

You'll need either Activator or SBT installed. We recommend Activator.

To install Activator, follow the instructions on the [get started](https://typesafe.com/platform/getstarted) page. After installing it, add the installation directory to your `PATH` or define the environment variable `ACTIVATOR_HOME` (MacOS, Linux, or Cygwin only).

If you prefer SBT and you need to install it, follow the instructions on the [download](http://www.scala-sbt.org/download.html) page. SBT puts itself on your path. However, if you have a custom installation that isn't on your path, define the environment variable `SBT_HOME` (MacOS, Linux, or Cygwin only).

## Setup for Hadoop Mode

If you want to run the examples on Hadoop, choose one of the following options.

The Hadoop vendors all provide virtual machine "sandboxes" that you can load into [VMWare](http://vmware.com), [VirtualBox](http://virtualbox.org), and other virtualization environments. Most now bundle Spark. Typesafe is working with some of the Hadoop vendors to bundle Activator and this workshop into customized versions of their sandboxes. Check the vendor's documentation.

If you have a Hadoop cluster installation or a "vanilla" virtual machine sandbox, verify if Spark is already installed. For example, log into a cluster node, edge node, or the sandbox and try running `spark-shell`. If Spark does not appear to be installed, your Hadoop vendor's web site should have information on installing and using Spark. In most cases, it will be as simple as downloading an appropriate build from the [Spark download](http://spark.apache.org/downloads.html) page. Select the distribution built for your Hadoop distribution.

Assuming you don't have administration rights, it's sufficient to expand the archive in your home directory on a cluster node, edge node, or within the sandbox. Then add the `bin` directory under the Spark installation directory to your `PATH` or define the environment variable `SPARK_HOME` to match the installation directory.

You'll need this workshop and Activator or SBT on the cluster or edge node, or in the sandbox. You can run Activator or SBT there or go through the workshop running on your local workstation, then move everything to the cluster or sandbox later to try out the Hadoop examples.

## Starting Activator

Change to the root directory for this workshop, either on your local machine or in your Hadoop sandbox, cluster node, or shell node.

To work on your local workstation, run `activator ui`, assuming it's in your path. If not, use the fully-qualified path to it. It will start Activator and open the web-based UI in your browser automatically. (If not, go to [localhost:8888](http://localhost:8888).)

If you prefer a command-line interface, either run `activator shell` or `sbt`.

If you have installed this workshop and Activator on a Hadoop cluster node or edge node, or in a sandbox virtual machine, change to the workshop root directory and run the command `./start.sh`. It will start Activator and load this workshop.

By default, it will also start the Activator web-based UI, but you'll have to open your browser to the correct URL yourself. See the message printed to the login console for the correct URL (including a different port number, 9999).

There are options for running in Activator *shell* mode or using SBT. Use `./start.sh --help` to see those options.

## Building and Testing

To ensure that the basic environment is working, compile the code and run the tests; use the Activator UI's <a class="shortcut" href="#test">test</a> link (or, if you're using the shell commands, run the `test` task). All dependencies are downloaded, the code is compiled, and the tests are executed. This will take a few minutes the first time and the tests should pass without error. (We've noticed that sometimes a timeout of some kind prevents the tests from completing successfully, but running the tests again works.)

Note that tests are provided for most, but not all of the examples. Also, they run Spark in *local mode*, in a single JVM process without using Hadoop. This is a convenient way to develop and test the logic of your applications before testing and deploying them in a cluster.

## Running the Examples

Next, let's run one of the examples both locally and with Hadoop to further confirm that everything is working.

## Running a Local-mode Example

In Activator, select the <a class="shortcut" href="#run">run</a> panel. Find the bullet item under *Main Class* for `WordCount3` and select it. (Unfortunately, they are not listed in alphabetical order.)

Click the *Start* button. The *Logs* panel shows the output as it runs. (If you're using the shell prompt, invoke `run-main WordCount3`.)

Note the `output` directory listed in the log messages. Use your login window (or create another one) to view the output in the directory, which will be `/root/spark-workshop/output/kjv-wc3`. You should find `_SUCCESS` and `part-00000` files, following Hadoop conventions, where the latter contains the actual data.

## Running a Hadoop Example

Now try the same program using Spark running in Hadoop. This time, select `hadoop.HWordCount3` in the *run* panel and click the *Start* button. There will be more log messages and it will take longer to run.

The log messages end with a URL where you can view the output in HDFS. If your VM's IP address is 192.168.64.100, for example, the URL will be `http://192.168.64.100:8000/filebrowser/#/user/root/output/kjv-wc3`. (Once you've opened this file browser, it will be easy to navigate to the outputs for the rest of the examples we'll run.) The same `_SUCCESS` and `part-00000` files will be found, although the lines in the latter might not be in the same order as for the *local* run.

Assuming you encountered no problems, everything is working!

> NOTE: The normal Hadoop and Spark convention is to never overwrite existing data. However, that would force us to manually delete old data before rerunning programs. So, to make the workshop easier, the programs delete existing data first.

## Naming Conventions

We're using a few conventions for the package structure and `main` class names:

* `AbcN` - The `Abc` program and the N^th^ example. With a few exceptions, it can be executed locally and in Hadoop. It defaults to local execution.
* `hadoop/HAbcN` - A driver program to run `AbcN` in Hadoop. These small classes use a Scala library API for managining operating system "processes". In this case, they invoke one or more shell scripts in the `scripts` directory, which in turn call the Spark driver program `$HOME/spark/bin/spark-submit`, passing it the correct arguments. We'll explore the details shortly.
* `solns/AbcNFooBarBaz` - The solution to the "foo bar baz" exercise that's built on `AbcN`. These programs can also be invoked from the *Run* panel.

Otherwise, we don't use package prefixes, but only because they tend to be inconvenient with the Activator's *Run* panel.


## The Exercises

Here is a list of the exercises. In subsequent sections, we'll dive into the details for each one. Note that each name ends with a number, indicating the order in which we'll discuss and try them:

* **Intro1:** The first example is actually run using the interactive `spark-shell`, as we'll see.
* **WordCount2:** The *Word Count* algorithm: Read a corpus of documents, tokenize it into words, and count the occurrences of all the words. A classic, simple algorithm used to learn many Big Data APIs. By default, this example uses a file containing the King James Version (KJV) of the Bible. (The `data` directory has a `README` that discusses the sources of the data files.) This example hard-codes various information, so we'll only run it in local mode.
* **WordCount3:** An alternative implementation of *Word Count* that uses a slightly different approach and also uses a small library to handle input command-line arguments, which we won't discuss in detail. This example and all subsequent examples run locally and in Hadoop.
* **Matrix4:** Demonstrates using explicit parallelism on a simplistic Matrix application.
* **Crawl5a:** Simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines. The documents "crawled" are sample emails from the Enron email dataset, each of which has been classified already as SPAM or HAM.
* **InvertedIndex5b:** Using the crawl data, compute the index of words to documents (emails).
* **NGrams6:** Find all N-word ("NGram") occurrences matching a pattern. In this case, the default is the 4-word phrases in the King James Version of the Bible of the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the NGram Phrase can also be a regular expression, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, and `hated`.
* **Joins7:** Spark supports SQL-style joins as shown in this simple example.
* **SparkStreaming8:** The streaming capability is relatively new and this exercise shows how it works to construct a simple "echo" server. Running it is a little more involved. See below.
* **SparkSQL9:** Uses the SparkSQL API to run basic queries over structured data, in this case, the same King James Version (KJV) of the Bible used in previous exercises.
* **hadoop/SparkSQLParquet10:** Demonstrates writing and reading [Parquet](http://parquet.io)-formatted data, a popular file format with good performance. This example and the next one are in a `hadoop` package, because they use features that are difficult to run without a build of Spark for a specific Hadoop cluster. In contrast, the previous examples work in "local" mode, as well.
* **hadoop/HiveSQL11:** A script that demonstrates interacting with Hive tables (we actually create one) in the Scala REPL!

Let's now work through these exercises...

## Intro1

[Intro1.scala](#code/src/main/scala/sparkworkshop/Intro1.scala)

Our first exercise demonstrates the useful *Spark Shell*, which is a customized version of Scala's REPL (read, eval, print, loop). We'll copy and paste some commands from the file [Intro1.scala](#code/src/main/scala/sparkworkshop/Intro1.scala).

The comments in this and the subsequent files try to explain the API calls being made.

> NOTE: while the extension is `.scala`, this file is not compiled with the rest of the code, because it works like a script. (The build is configured to not compile this file.)

## Hadoop Execution

Log into the VM as `root` so you can run a shell script we've provided. In the `root` user's home folder `/root` and run the following command:

```
spark-workshop/scripts/sparkshell.sh
```

This script calls the actual Spark Shell script, `$HOME/spark/bin/spark-shell` and passes a `--jars` argument with the package of the workshop code, as well as any additional arguments you pass to the script. (Try the `--help` option to see the full list.)

You'll see a lot of log messages, ending with the Scala REPL prompt `scala>`.

We're going to paste in the code from [Intro1.scala](#code/src/main/scala/sparkworkshop/Intro1.scala). Click the link to view it.

We'll paste a few lines at a time so we can discuss them. (It's harmless to paste comments from the source file.)

First, there are some commented lines that every Spark program needs, but the Spark Shell does automatically for you:

```
// import org.apache.spark.SparkContext
// import org.apache.spark.SparkContext._
// val sc = new SparkContext("local", "Intro (1)")
```

The [SparkContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext) drives everything else. Why are there two, very similar `import` statements? The first one imports the `SparkContext` type so it wouldn't be necessary to use a fully-qualified name in the `new SparkContext` statement. The second import statement is analogous to a `static import` in Java, where we make some methods and values visible in the current scope, again without requiring qualification.

When a `SparkContext` is constructed, there are several constructors that can be used. This one takes a string for the "master" and a job name. The master must be one of the following:

* `local`: Start the Spark job standalone and use a single thread to run the job.
* `local[k]`: Use `k` threads instead. Should be less than the number of cores.
* `mesos://host:port`: Connect to a running, Mesos-managed Spark cluster.
* `spark://host:port`: Connect to a running, standalone Spark cluster.
* `yarn-client` or `yarn-cluster`: Connect to a YARN cluster.

```
val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache
```

Define a read-only variable `input` of type [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) (inferred) by loading the text of the King James Version of the Bible, which has each verse on a line, we then map over the lines converting the text to lower case.

> The `data` directory has a `README` that discusses the files present and where they came from.

Finally, we cache the data in memory for faster, repeated retrieval. You shouldn't always do this, as it consumes memory, but when your workflow will repeatedly reread the data, caching provides performance improvements.

Next, let's work with the data:

```
val sins = input.filter(line => line.contains("sin"))
val count = sins.count()         // How many sins?
val array = sins.collect()       // Convert the RDD into a collection (array)
array.take(20) foreach println   // Take the first 20, and print them 1/line.
```

Filter the input for just those verses that mention "sin" (recall that the text is now lower case). Then count how many were found, convert the RDD to a Scala collection (in the memory for the driver process JVM). Finally, loop through the first twenty lines, printing each one.

Note: in Scala, the `()` in method calls are actually optional for no-argument methods.

You can define *functions* as *values*:

```
val filterFunc: String => Boolean =
    (s:String) => s.contains("god") || s.contains("christ")
```

An alternative approach; create a separate filter function instead and pass it as an argument to the filter method. Specifically, `filterFunc` is a value that's a function of type `String` to `Boolean`.

The following more concise form is equivalent, due to *type inference*:

```
val filterFunc: String => Boolean =
    s => s.contains("god") || s.contains("christ")
```

Now use the filter to find all the `sins` verses that mention God or Christ, then count them. (Note that we dropped the parentheses after "count" in this case.)

```
val sinsPlusGodOrChrist  = sins filter filterFunc
val countPlusGodOrChrist = sinsPlusGodOrChrist.count
```

A non-script program should gracefully shutdown, but we don't need to do so here.

```
// sc.stop()
```

If you exit the REPL immediately, this will happen implicitly. Still, it's a good practice to always call `stop`.

There are comments at the end of each source file, including this one, with suggested exercises to learn the API. Try them as time permits. Solutions for some exercises are provided in the `src/main/scala/sparkworkshop/solns` directory.

You can exit the shell now. Type `:quit` or use `^d` (control-d).

## Local Mode Execution

For local mode, you can use Activator's `console` task, because we've configured it to work similarly to Spark's shell. If you're running the Activator shell, you might try repeating the previous session, but this time you'll be running in local mode. Otherwise, skip to the next section.

In the Activator shell, type `console`. You'll see fewer log messages this time, ending with the same `scala>` prompt.  Try pasting in the same code snippets from `Intro1.scala`.

## WordCount2

[WordCount2.scala](#code/src/main/scala/sparkworkshop/WordCount2.scala)

The classic, simple *Word Count* algorithm is easy to understand and it's suitable for parallel computation, so it's a good vehicle when first learning a Big Data API.

In *Word Count*, you read a corpus of documents, tokenize each one into words, and count the occurrences of all the words globally. The initial reading, tokenization, and "sub-counts" can be done in parallel.

Now we'll return to the Activator UI (or shell).

[WordCount2.scala](#code/src/main/scala/sparkworkshop/WordCount2.scala) uses the KJV Bible text again. (Subsequent exercises will add the ability to override defaults with command-line arguments.)

If using the <a class="shortcut" href="#run">run</a> panel, select `WordCount2` and click the "Start" button. The messages in the "Logs" panel lists the "output" directory, which is in the *local* file system, not HDFS. In another terminal window, you can view the files in this directory, an empty `_SUCCESS` file that marks completion and a `part-00000` file that contains the data.

As before, here is the text of the script in sections, with code comments removed:

```
import com.typesafe.sparkworkshop.util.FileUtil
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

We use the Java default package for the compiled exercises, but you would normally organize your applications in packages, in the usual way. We start by importing a `FileUtil` class we use for "housekeeping". Then we use the same two `SparkContext` imports we discussed previously.

> Even though most of the examples and exercises from now on will be compiled classes, you could still use the Spark Shell to try out most constructs. This is especially useful when debugging and experimenting!

Here is the outline of the rest of the program, demonstrating a pattern we'll use throughout.

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
```

In case the script fails with an exception, putting the `SparkContext.stop()` inside a `finally` clause ensures that will get invoked no matter what happens.

The content of the `try` clause is the following:

```
val out = "output/kjv-wc2"
// Deleting old output (if any)
FileUtil.rmrf(out)

val input = sc.textFile("data/kjvdat.txt").map(line => line.toLowerCase)
input.cache

val wc = input
  .flatMap(line => line.split("""\W+"""))
  .map(word => (word, 1))
  .reduceByKey((count1, count2) => count1 + count2)

println(s"Writing output to: $out")
wc.saveAsTextFile(out)
```

Because Spark follows Hadoop conventions that it won't overwrite existing data, we delete any previous output, if any. Of course, *you should not do this* in most of your production scripts!

Next we load and cache the data like we did in the first example.

Now we setup a pipeline of operations to perform the word count.

First the line is split into words using as the separator any run of characters that isn't alphanumeric (e.g., whitespace and punctuation). This also conveniently removes the trailing `~` characters at the end of each line that exist in the file for some reason. `input.flatMap(line => line.split(...))` maps over each line, expanding it into a collection of words, yielding a collection of collections of words. The `flat` part flattens those nested collections into a single, "flat" collection of words.

The next two lines convert the single word "records" into tuples with the word and a count of `1`. In Shark, the first field in a tuple will be used as the default key for joins, group-bys, and the `reduceByKey` we use next.

The `reduceByKey` step effectively groups all the tuples together with the same word (the key) and then "reduces" the values using the passed in function. In this case, the two counts are added together.

Finally, we invoke `saveAsTextFile` to write the final RDD to the output location.

Note that the input and output locations will be relative to the local file system, when running in local mode, and relative to user's home directory in HDFS (e.g., `/user/$USER`), when the program runs in Hadoop.

Spark also follows another Hadoop convention for file I/O; the `out` path is actually interpreted as a directory name. Here is its contents after running in local mode (the default):

```
$ ls -l output/kjv-wc2
total 328
-rwxrwxrwx  1 deanwampler  staff       0 May  3 09:40 _SUCCESS
-rwxrwxrwx  1 deanwampler  staff  158620 May  3 09:40 part-00000
```

In a real cluster with lots of data and lots of concurrent tasks (JVM processes), there would be many `part-NNNNN` files. They contain the actual data. The `_SUCCESS` file is a useful convention that signals the end of processing. It's useful because tools that are watching for the data to be written so they can perform subsequent processing will know the files are complete when they see this marker file.

**Quiz:** If you look at the (unsorted) data, you'll find a lot of entries where the word is a number. (Try "grepping" to find them.) Are there really that many numbers in the bible? If not, where did the numbers come from? Look at the original file for clues.


## Exercises

At the end of each example source file, you'll find exercises you can try. Solutions for some of them are implemented in the `solns` package. For example, [solns/WordCount2GroupBy.scala](#code/src/main/scala/sparkworkshop/solns/WordCount2GroupBy.scala)
solves a "group by" exercise.

You'll need to edit the source code on the VM somehow. One way is to simply use `vi` to edit the code, then use the Activator UI to run it. (The *Run* command compiles the changes first.) Another approach is to use the secure copy command, `scp`, to copy the sources to your workstation, edit them there, then copy them back.

The best approach is to share one or more directories between your workstation and the VM Linux instance. This will allow you to edit the code in your workstation environment with the changes immediately available in the VM. See the documentation for your VM runner for details.

## WordCount3

[WordCount3.scala](#code/src/main/scala/sparkworkshop/WordCount3.scala)

This exercise also implements *Word Count*, but it uses a slightly simpler approach. It also uses a utility library we added to handle input command-line arguments, demonstrating some idiomatic (but fairly advanced) Scala code. We won't worry about the details of this utility code too much.

We'll run this example in both local mode and in Hadoop (YARN).

This version also does some data cleansing to improve the results. The sacred text files included in the `data` directory, such as `kjvdat.txt` are actually formatted records of the form:

```
book|chapter#|verse#|text
```

That is, pipe-separated fields with the book of the Bible (e.g., Genesis, but abbreviated "Gen"), the chapter and verse numbers, and then the verse text. We just want to count words in the verses, although including the book names wouldn't change the results significantly. (Now you can figure out the answer to the "quiz" in the previous section...)

Command line options can be used to override the defaults. You'll have to use the Activator shell from a login window to use this feature, and you'll have to use the `run-main` task, which lets us specify a particular `main` to run and optional arguments. We have also defined some shortcut command aliases for many of the programs. In this case, you can either enter `run-main WordCount3` at the activator shell prompt. Or, you can use the alias `ex3`. Either way, you can add options after these terms.

The "\" characters in the following and subsequent command descriptions are used to indicate long lines that I wrapped to fit. Enter the commands on a single line without the "\". Following Unix conventions, `[...]` indicate optional arguments, and `|` indicate alternatives:

```
run-main WordCount3 [ -h | --help] \
  [-i | --in | --inpath input] \
  [-o | --out | --outpath output] \
  [-m | --master master] \
  [-q | --quiet]
```

Where the options have the following meanings:

```
-h | --help     Show help and exit.
-i ... input    Read this input source (default: data/kjvdat.txt).
-o ... output   Write to this output location (default: output/kjvdat-wc3).
-m ... master   local, local[k], yarn-client, etc., as discussed previously.
-q | --quiet    Suppress some informational output.
```

Here is an example that simply specifies all the default values for the options (again, all on one line without the "\"):

```
run-main WordCount3 \
  --inpath data/kjvdat.txt --output output/kjv-wc3 \
  --master local
```

You can try different variants of `local[k]` for the `master` option, but keep `k` less than the number of cores in your machine.

As before, you can run with the default arguments using the *Run* panel in the Activator UI. If you use the Activator shell command line, you would use `run-main WordCount3` with or without options. There is also a command alias for these terms, `ex3`. We'll discuss the options for running in Hadoop in the next section.

The `input` and `master` arguments are basically the same things we discussed for `WordCount2`, but the `output` argument.

When you specify an input path for Spark, you can specify `bash`-style "globs" and even a list of them:

* `data/foo`: Just the file `foo` or if it's a directory, all its files, one level deep (unless the program does some extra handling itself).
* `data/foo*.txt`: All files in `data` whose names start with `foo` and end with the `.txt` extension.
* `data/foo*.txt,data2/bar*.dat`: A comma-separated list of globs.

Okay, with that out of the way, let's walk through the implementation of `WordCount3`, in sections:

```
import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
```

As before, but with our new `CommandLineOptions` utilities added.

```
object WordCount3 {
  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      CommandLineOptions.outputPath("output/kjv-wc3"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master").toString
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path").toString
```
I won't discuss the implementation of [CommandLineOptions.scala](#code/src/main/scala/sparkworkshop/util/CommandLineOptions.scala) except to say that it defines some methods that create instances of an `Opt` type, one for each of the options we discussed above. The single argument given to some of the methods (e.g., `CommandLineOptions.inputPath("data/kjvdat.txt")`) specifies the default value for that option.

After parsing the options, we extract some of the values we need.

Next, if we're running in local mode, we delete the old output, if any. Recall that we do this so it's more convenient to run the program repeatedly, but normally you wouldn't want to delete data automatically! We will also delete old data from HDFS when running in Hadoop, but this is handled through a different mechanism, as we'll see shortly.

```
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }
```

Now we create the `SparkContext` with the desired `master` setting. Then we process the input as before, with one change...

```
    val sc = new SparkContext(master, "Word Count (3)")

    try {
      val input = sc.textFile(argz("input-path").toString)
        .map(line => line.toLowerCase.split("\\s*\\|\\s*").last)
      input.cache
```

It starts out much like `WordCount2`, but it splits each line into fields, where the lines are of the form: `book|chapter#|verse#|text`. Only the text of each verse is kept this time. As before, the `input` reference is an [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) that we then cache. Note that calling `last` on the split array is robust even for lines that don't have the delimiter, if there are any; it simply returns the whole original string.

```
      val wc2 = input
        .flatMap(line => line.split("""\W+"""))
        .countByValue()  // Returns a Map[T, Long]
```

Take input and split on non-alphanumeric sequences of character as we did in `WordCount2`, but rather than map to `(word, 1)` tuples and use `reduceByKey`, we simply treat the words as values and call `countByValue` to count the unique occurrences. Hence, a simpler (and probably more efficient) approach.

```
      val wc2b = wc2a.map(key_value => s"${key_value._1},${key_value._2}").toSeq
      val wc2 = sc.makeRDD(wc2b, 1)

      if (!quiet) println(s"Writing output to: $out")
      wc2.saveAsTextFile(out)

    } finally {
      sc.stop()
    }
  }
}
```

The result of `countByValue` is a Scala `Map`, not an [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD), so we format the key-value pairs into a sequence of strings in comma-separated value (CSV) format. The we convert this sequence back to an [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) with `makeRDD`. Finally, we save to a text file as before.

## Running as a Hadoop Job

So, running this program in Activator will run locally. Let's now run it in Hadoop. The data is already copied to HDFS. All we need to do is run a different driver program, [hadoop.HWordCound3](#code/src/main/scala/sparkworkshop/hadoop.HWordCound3.scala), which is also available in the *Run* panel. Try it now.

The output is more verbose and the execution time is longer, due to Hadoop's overhead. The end of the output shows a URL for the *Hue* UI that's also part of the Sandbox. Open your browser to that URL to look at the data. The content will be very similar to the output of the previous, local run, but it will be formatted differently and the word-count pairs will be in a different (random) order.

Using the Activator shell, you can use `run-main hadoop.HWordCount3` or the alias `hex3`.

For convenient, there is also a bash shell script for this exercise in the `scripts` directory under the `spark-workshop` home, [scripts.wordcount3.sh](#code/scripts.wordcount3.sh). The other exercises also have corresponding scripts. The `wordcount3.sh` script looks like this:

```
#!/bin/bash

output=output/kjv-wc3
dir=$(dirname $0)
$dir/hadoop.sh --class WordCount3 --output "$output" "$@"
```

It calls a [scripts.hadoop.sh](#code/scripts.hadoop.sh) script in the same directory, which deletes the old output from HDFS, if any, and calls Spark's `$SPARK_HOME/bin/spark-submit` to submit the job to YARN. One of the arguments it passes to `spark-submit` is the jar file containing all the project code. This jar file is built automatically anytime to you invoke the Activator *run* command.

Let's return to [hadoop.HWordCound3](#code/src/main/scala/sparkworkshop/hadoop.HWordCound3.scala), which is quite small:

```
package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

object HWordCount3 {
  def main(args: Array[String]): Unit = {
    Hadoop("WordCount3", "output/kjv-wc3", args)
  }
}
```

It accepts the same options as `WordCount3`, although the `--master` option is automatically supplied to specify YARN (overriding the default `local` value). This will be true of the Hadoop drivers classes for the other examples, too.

It delegates to a helper class [com.typesafe.sparkworkshop.util.Hadoop](#code/src/main/scala/com/typesafe/sparkworkshop/util/Hadoop.scala) to do the work. It passes the class name and the output, which will be used to delete old output. This is a hack: the default output path must be specified here, even though the same default value is also encoded in the application. This is because we have to pass it to the `hadoop.sh` script.

Here is the `Hadoop` helper class:

```
package com.typesafe.sparkworkshop.util
import scala.sys.process._

object Hadoop {
  def apply(className: String, defaultOutpath: String, args: Array[String]): Unit = {
    val user = sys.env.get("USER") match {
      case Some(user) => user
      case None =>
        println("ERROR: USER environment variable isn't defined. Using root!")
        "root"
    }

    // Did the user specify an output path? Use it instead.
    val predicate = (arg: String) => arg.startsWith("-o") || arg.startsWith("--o")
    val args2 = args.dropWhile(arg => !predicate(arg))
    val outpath = if (args2.size == 0) s"/user/$user/$defaultOutpath"
      else if (args2(1).startsWith("/")) args2(1)
      else s"/user/$user/${args(2)}"

    // We don't need to remove the output argument. A redundant occurrence
    // is harmless.
    val argsString = args.mkString(" ")
    val exitCode = s"scripts/hadoop.sh --class $className --out $outpath $argsString".!
    if (exitCode != 0) sys.exit(exitCode)
  }
}
```

It tries to determine the user name and whether or not the user explicitly specified an output argument, which should override the hard-coded value.

Finally, it invokes the [scripts.hadoop.sh](#code/scripts.hadoop.sh) `bash` script we mentioned above, so that we go thorugh Spark's `spark-submit` script for submitting to the Hadoop YARN cluster.

Don't forget the try the exercises at the end of the source file.

## Matrix4

[Matrix4.scala](#code/src/main/scala/sparkworkshop/Matrix4.scala)

An early use for Spark was implementing Machine Learning algorithms. Spark's `MLlib` of algorithms contains classes for vectors and matrices, which are important for many ML algorithms. This exercise uses a simple representation of matrices to explore another topic; explicit parallelism.

The sample data is generated internally; there is no input that is read. The output is written to the file system as before.

Here is the `run-main` command with optional arguments:

```
run-main Matrix4 [ -h | --help] \
  [-d | --dims NxM] \
  [-o | --out | --outpath output] \
  [-m | --master master] \
  [-q | --quiet]
```

The one new optin is for specifying the dimensions, where the string `NxM` is parsed to mean `N` rows and `M` columns. The default is `5x10`.

Like for `WordCount3`, there is also a `ex4` short cut for `run-main Matrix4` and you can run with the default arguments using the Activator *Run* panel.

For Hadoop, select and run [hadoop.HMatrix4](#code/src/main/scala/sparkworkshop/hadoop.HMatrix4.scala) in the UI, use `run-main hadoop.HMatrix4` or `hex4` in the Activator shell (both of which accept the same list of options, except for `--master` which is already provided), and there is a bash script [scripts/matrix4.sh](#code/scripts/matrix4.sh).

We won't cover all the code, skipping the familiar stuff:

```
import com.typesafe.sparkworkshop.util.Matrix
...

object Matrix4 {

  case class Dimensions(m: Int, n: Int)

  def main(args: Array[String]) = {

    val options = CommandLineOptions(...)
    val argz   = options(args.toList)
    ...

    val dimsRE = """(\d+)\s*x\s*(\d+)""".r
    val dimensions = argz("dims").toString match {
      case dimsRE(m, n) => Dimensions(m.toInt, n.toInt)
      case s =>
        println("""Expected matrix dimensions 'NxM', but got this: $s""")
        sys.exit(1)
    }
```

`Dimensions` is a convenience class for capturing the default or user-specified matrix dimensions. We parse the argument string to extract `N` and `M`, then construct a `Dimension` instance.

```
    val sc = new SparkContext("local", "Matrix (4)")

    try {
      // Set up a mxn matrix of numbers.
      val matrix = Matrix(dimensions.m, dimensions.n)

      // Average rows of the matrix in parallel:
      val sums_avgs = sc.parallelize(1 to dimensions.m).map { i =>
        // Matrix indices count from 0.
        // "_ + _" is the same as "(count1, count2) => count1 + count2".
        val sum = matrix(i-1) reduce (_ + _)
        (sum, sum/dimensions.n)
      }.collect    // convert to an array
```

The core of this example is the use of `SparkContext.parallelize` to process each row in parallel (subject to the available cores on the machine or cluster, of course). In this case, we sum the values in each row and compute the average.

The argument to `parallelize` is a sequence of "things" where each one will be passed to one of the operations. Here, we just use the literal syntax to construct a sequence of integers from 1 to the number of rows. When the anonymous function is called, one of those row numbers will get assigned to `i`. We then grab the `i-1` row (because of zero indexing) and use the `reduce` method to sum the column elements. A final tuple with the sum and the average is returned.

```
      // Make a new sequence of strings with the formatted output, then we'll
      // dump to the output location.
      val outputLines = Vector(          // Scala's Vector, not MLlib's version!
        s"${dimensions.m}x${dimensions.n} Matrix:") ++ sums_avgs.zipWithIndex.map {
        case ((sum, avg), index) =>
          f"Row #${index}%2d: Sum = ${sum}%4d, Avg = ${avg}%3d"
      }
      val output = sc.makeRDD(outputLines)  // convert back to an RDD
      if (!quiet) println(s"Writing output to: $out")
      output.saveAsTextFile(out)
    } finally { ... }
  }
}
```

The output is formatted as a sequence of strings and converted back to an [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) for output. The expression `sums_avgs.zipWithIndex` creates a tuple with each `sums_avgs` value and it's index into the collection. We use that to add the row index to the output.

## Crawl5a

[Crawl5a.scala](#code/src/main/scala/sparkworkshop/Crawl5a.scala)

The fifth example is in two-parts. The first part simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines, from words to documents. The documents "crawled" are sample emails from the Enron email dataset, each of which has been previously classified already as SPAM or HAM.

`Crawl5a` supports the same command-line options as `WordCount3`:

```
run-main Crawl5a [ -h | --help] \
  [-i | --in | --inpath input] \
  [-o | --out | --outpath output] \
  [-m | --master master] \
  [-q | --quiet]
```

As before, there is also a `ex5a` short cut for `run-main Crawl5a` and you can run with the default arguments using the Activator *Run* panel.

This version of the program doesn't work correctly with HDFS, in part because it uses Java I/O to work a POSIX-compatible file system and HDFS isn't a POSIX file system. So, there's a second version called `Crawl5aHDFS` that uses a different Spark API call to ingest the files. Unfortunately, this call doesn't work with local file systems, so we need two programs. `Crawl5aHDFS` supports the same options that `Crawl5a` supports.

So, for Hadoop, select and run [hadoop.HCrawl5aHDFS](#code/src/main/scala/sparkworkshop/hadoop.HCrawl5aHDFS.scala) in the UI, use `run-main hadoop.HCrawl5aHDFS` or `hex5a` in the Activator shell. There is also a bash script [scripts/crawlhdfs5a.sh](#code/scripts/crawlhdfs5a.sh).

Most of the code is straightforward. The comments explain the details.

The output format is demonstrated with the following line from the output `(email_file_name, text)`:

```
(0038.2001-08-05.SA_and_HP.spam.txt,  Subject: free foreign currency newsletter ...)
```

The next step has to parse this format to generate the *inverted index*.

## InvertedIndex5b

[InvertedIndex5b.scala](#code/src/main/scala/sparkworkshop/InvertedIndex5b.scala)

Using the crawl data just generated, compute the index of words to documents (emails).

`InvertedIndex5b` supports the usual command-line options:

```
run-main InvertedIndex5b [ -h | --help] \
  [-i | --in | --inpath input] \
  [-o | --out | --outpath output] \
  [-m | --master master] \
  [-q | --quiet]
```

For Hadoop, select and run [hadoop.HInvertedIndex5b](#code/src/main/scala/sparkworkshop/hadoop.HInvertedIndex5b.scala) in the UI, use `run-main hadoop.HInvertedIndex5b` or `hex5b` in the Activator shell. There is also a bash script [scripts/invertedindex5b.sh](#code/scripts/invertedindex5b.sh).

The code outside the usual `try` clause follows the usual pattern, so we'll focus on the contents of the `try` clause:

```
try {
  val lineRE = """^\s*\(([^,]+),(.*)\)\s*$""".r
  val input = sc.textFile(argz("input-path").toString) map {
    case lineRE(name, text) => (name.trim, text.toLowerCase)
    case badLine =>
      Console.err.println("Unexpected line: $badLine")
      ("", "")
  }
```

We load the "crawl" data, where each line was written by `Crawl5a` with the following format: `(document_id, text)` (including the parentheses). Hence, we use a regular expression with "capture groups" to extract the `document_id` and `text`.

Note the function passed to `map`. It has the form:

```
{
  case lineRE(name, text) => ...
  case line => ...
}
```

There is now explicit argument list like we've used before. This syntax is the literal syntax for a *partial function*, a mathematical concept for a function that is not defined at all of its inputs. It is implemented with Scala's `PartialFunction` type.

We have two `case` match clauses, one for when the regular expression successfully matches and returns the capture groups into variables `name` and `text` and the second which will match everything else, assigning the line to the variable `badLine`. (In fact, this catch-all clause makes the function *total*, not *partial*.) The function must return a two-element tuple, so the catch clause simply returns `("","")`.

Note that the specified or default `input-path` is a directory with Hadoop-style content, as discussed previously. Spark knows to ignore the "hidden" files.

The embedded comments in the rest of the code explains each step:

```
if (!quiet)  println(s"Writing output to: $out")

// Split on non-alphanumeric sequences of character as before.
// Rather than map to "(word, 1)" tuples, we treat the words by values
// and count the unique occurrences.
input
  .flatMap {
    // all lines are two-tuples; extract the path and text into variables
    // named "path" and "text".
    case (path, text) =>
      // If we don't trim leading whitespace, the regex split creates
      // an undesired leading "" word!
      text.trim.split("""\W+""") map (word => (word, path))
  }
  .map {
    // We're going to use the (word, path) tuple as a key for counting
    // all of them that are the same. So, create a new tuple with the
    // pair as the key and an initial count of "1".
    case (word, path) => ((word, path), 1)
  }
  .reduceByKey{    // Count the equal (word, path) pairs, as before
    case (count1, count2) => count1 + count2
  }
  .map {           // Rearrange the tuples; word is now the key we want.
    case ((word, path), n) => (word, (path, n))
  }
  .groupBy {       // Group the same words together
    case (word, (path, n)) => word
  }
  .map {           // reformat the output; make string of the groups.
    case (word, seq) =>
      val seq2 = seq map {
        case (redundantWord, (path, n)) => (path, n)
      }
      (word, seq2.mkString(", "))
  }
  .saveAsTextFile(out)
} finally { ... }
```

Each output record has the following form: `(word, (doc1, n1), (doc2, n2), ...)`. For example, the word "ability" appears twice in one email and once in another (both SPAM):

```
(ability,(0018.2003-12-18.GP.spam.txt,2), (0020.2001-07-28.SA_and_HP.spam.txt,1))
```

It's worth studying this sequence of transformations to understand how it works. Many problems can be solves with these techniques. You might try reading a smaller input file (say the first 5 lines of the crawl output), then hack on the script to dump the [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) after each step.

A few useful [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) methods for exploration include `RDD.sample` or `RDD.take`, to select a subset of elements. Use `RDD.saveAsTextFile` to write to a file or use `RDD.collect` to convert the RDD data into a "regular" Scala collection and then use `println`, etc. to dump the contents.

## NGrams6

[NGrams6.scala](#code/src/main/scala/sparkworkshop/NGrams6.scala)

In *Natural Language Processing*, one goal is to determine the sentiment or meaning of text. One technique that helps do this is to locate the most frequently-occurring, N-word phrases, or *NGrams*. Longer NGrams can convey more meaning, but they occur less frequently so all of them appear important. Shorter NGrams have better statistics, but each one conveys less meaning. In most cases, N = 3-5 appears to provide the best balance.

This exercise finds all NGrams matching a user-specified pattern. The default is the 4-word phrases the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the user can also specify an NGram Phrase that is a regular expression or a mixture, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, or `hated` as the second word.

`NGrams6` supports the same command-line options as `WordCount3`, except for the output path (it just writes to the console), plus two new options:

```
run-main NGrams6 [ -h | --help] \
  [-i | --in | --inpath input] \
  [-m | --master master] \
  [-c | --count N] \
  [-n | --ngrams string] \
  [-q | --quiet]
```

Where

```
-c | --count N        List the N most frequently occurring NGrams (default: 100)
-n | --ngrams string  Match string (default "% love % %"). Quote the string!
```

For Hadoop, select and run [hadoop.HNGrams6](#code/src/main/scala/sparkworkshop/hadoop.HNGrams6.scala) in the UI, use `run-main hadoop.HNGrams6` or `hex6` in the Activator shell. There is also a bash script [scripts/ngrams6.sh](#code/scripts/ngrams6.sh).

I'm in yür codez:

```
...
val ngramsStr = argz("ngrams").toString.toLowerCase
val ngramsRE = ngramsStr.replaceAll("%", """\\w+""").replaceAll("\\s+", """\\s+""").r
val n = argz("count").toInt
```

From the two new options, we get the `ngrams` string. Each `%` is replaced by a regex to match a word and whitespace is replaced by a general regex for whitespace.

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

  out.println(s"Found ${ngramz.size} ngrams:")
  ngramz foreach {
    case (ngram, count) => out.println("%30s\t%d".format(ngram, count))
  }
} finally { ... }
```

We need an implementation of `Ordering` to sort our found NGrams descending by count.
We read the data as before, but note that because of our line orientation, we *won't* find NGrams that cross line boundaries! This doesn't matter for our sacred text files, since it wouldn't make sense to find NGrams across verse boundaries, but a more flexible implementation should account for this. Note that we also look at just the verse text, as in `WordCount3`.

The `map` and `reduceByKey` calls are just like we used previously for `WordCount2`, but now we're counting found NGrams. The final `takeOrdered` call combines sorting with taking the top `n` found. This is more efficient than separate sort, then take operations. As a rule, when you see a method that does two things like this, it's usually there for efficiency reasons!

## Joins7

[Joins7.scala](#code/src/main/scala/sparkworkshop/Joins7.scala)

Joins are a familiar concept in databases and Spark supports them, too. Joins at very large scale can be quite expensive, although a number of optimizations have been developed, some of which require programmer intervention to use. We won't discuss the details here, but it's worth reading how joins are implemented in various *Big Data* systems, such as [this discussion for Hive joins](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Joins#LanguageManualJoins-JoinOptimization) and the **Joins** section of [Hadoop: The Definitive Guide](http://shop.oreilly.com/product/0636920021773.do).

Here, we will join the KJV Bible data with a small "table" that maps the book abbreviations to the full names, e.g., `Gen` to `Genesis`.

`Joins7` supports the following command-line options:

```
run-main Joins7 [ -h | --help] \
  [-i | --in | --inpath input] \
  [-o | --out | --outpath output] \
  [-m | --master master] \
  [-a | --abbreviations path] \
  [-q | --quiet]
```

Where the `--abbreviations` is the path to the file with book abbreviations to book names. It defaults to `data/abbrevs-to-names.tsv`. Note that the format is tab-separated values, which the script must handle correctly.

For Hadoop, select and run [hadoop.HJoins7](#code/src/main/scala/sparkworkshop/hadoop.HJoins7.scala) in the UI, use `run-main hadoop.HJoins7` or `hex7` in the Activator shell. There is also a bash script [scripts/joins7.sh](#code/scripts/joins7.sh).

Here r yür codez:

```
...
try {
  val input = sc.textFile(argz("input-path").toString)
    .map { line =>
      val ary = line.split("\\s*\\|\\s*")
      (ary(0), (ary(1), ary(2), ary(3)))
    }
```

The input sacred text (default: `data/kjvdat.txt`) is assumed to have the format `book|chapter#|verse#|text`. We split on the delimiter and output a two-element tuple with the book abbreviation as the first element and a nested tuple with the other three elements. For joins, Spark wants a `(key,value)` tuple, which is why we use the nested tuple.

The abbreviations file is handled similarly, but the delimiter is a tab:

```
val abbrevs = sc.textFile(argz("abbreviations").toString)
  .map{ line =>
    val ary = line.split("\\s+", 2)
    (ary(0), ary(1).trim)  // I've noticed trailing whitespace...
  }
```

Note the second argument to `split`. Just in case a full book name has a nested tab, we explicitly only want to split on the first tab found, yielding two strings.

```
// Cache both RDDs in memory for fast, repeated access.
input.cache
abbrevs.cache

// Join on the key, the first field in the tuples; the book abbreviation.

val verses = input.join(abbrevs)

if (input.count != verses.count) {
  println(s"input count != verses count (${input.count} != ${verses.count})")
}
```

We perform an inner join on the keys of each RDD and add a sanity check for the output. Since this is an inner join, the sanity check catches the case where an abbreviation wasn't found and the corresponding verses were dropped!

The schema of `verses` is this: `(key, (value1, value2))`, where `value1` is `(chapter, verse, text)` from the KJV input and `value2` is the full book name, the second "field" from the abbreviations file. We now flatten the records to the final desired form, `fullBookName|chapter|verse|text`:

```
val verses2 = verses map {
  // Drop the key - the abbreviated book name
  case (_, ((chapter, verse, text), fullBookName)) =>
    (fullBookName, chapter, verse, text)
}
```

Lastly, we write the output:

```
  if (!quiet) println(s"Writing output to: $out")
  verses2.saveAsTextFile(out)
} finally { ... }
```

The `join` method we used is implemented by [spark.rdd.PairRDDFunctions](http://spark.apache.org/docs/1.0.0/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions), with many other methods for computing "co-groups", outer joins, etc.

You can verify that the output file looks like the input KJV file with the book abbreviations replaced with the full names. However, as currently written, the books are not retained in the correct order! (See the exercises in the source file.)

## SparkStreaming8

[SparkStreaming8.scala](#code/src/main/scala/sparkworkshop/SparkStreaming8.scala)

The streaming capability is relatively new and this exercise uses it to construct a simple "word count" server. The example has two running modes. The default mode just reads the contents of a file (the KJV Bible file, by default). That works best in Activator using the "run" command. The other mode ingests "events" over a socket. In either case, the data is captured in 1-second intervals (called *batches*) and word count it performed on the data in the batch.

Let's start with the mode that just reads a file. In Activator, run `SparkStreaming8` as we've done for the other exercises. It will read the KJV Bible text file, then terminate after 5 seconds, because otherwise the app will run forever, waiting for a changed text file to appear!

If you look in the local `output` directory, you'll see many subdirectories of the form `output/kjv-wc-streaming-<timestamp>.out/`, where the timestamp will be milliseconds that increment by 1000 at a time. In Spark Streaming, each batch (time interval of events) is written to a new, timestamped directory when output methods are called. The `--out` argument to the program is used as the directory name prefix. Most of the `part-0000N` files (you'll probably see two per directory) will be empty, because most or all of the KJV input is handled in the first batch. `SparkStreaming8` automatically terminates after a few seconds, so it doesn't run forever, even when the input is exhausted!

Alternatively, the app can receive text over a socket connection. To do this, you'll need to log into two terminal window in the VM.

We'll use the `nc` command to send data to the spark streaming process (part of [NetCat](http://netcat.sourceforge.net/)).

To try the streaming program with a socket connection, run the following command in one of the terminal windows:

```
nc -l 9900
```

It will wait for connections (the `-l` option) on port 9900. Type in several lines of random text, with line feeds. They will be read by the streaming program when it connects.

Now run `SparkStreaming8` with one of the following scripts:

* `scripts/sparkstreaming8-local.sh` - Run streaming in local mode.
* `scripts/sparkstreaming8.sh` - Run streaming in Hadoop.

The "local" version starts Activator in shell mode and then invokes a `run-main` command. You may need to terminate the UI process if it's running. Control-C is your friend. ;) Here is the command that's executed in the Activator shell:

```
run-main SparkStreaming8 --socket localhost:9900 --out output/socket-streaming
```

Once it starts you'll see similar output, where it dumps the garbage you typed into the `nc` window. Once `SparkStreaming8` exits, so will `nc`. Activator will exit, too. Look at the output directories created. Most files will be empty.

Once again, `SparkStreaming8` will terminate automatically after 5 seconds, even if you keep inputing text into `nc`. We'll discuss an override in a moment.

Do these steps again with the Hadoop version, `scripts/sparkstreaming8.sh`. This script works just like the other Hadoop scripts we described previously. It invokes `$SPARK_HOME/bin/spark-submit`. Don't forget to start `nc` again.

What if kill the `nc` process? `SparkStreaming8` detects that the socket dropped and starts shutting down, which takes a few moments. The program has a "listener" that
triggers shutdown when the socket drops. However, by default, Spark Streaming will attempt to re-establish the connection, which is probably what you would want in a real, long-running system.

`SparkStreaming8` supports the following command-line options:

```
run-main SparkStreaming8 [ -h | --help] \
  [-i | --in | --inpath input] \
  [-s | --socket server:port] \
  [-n | --no | --no-term] \
  [-q | --quiet]
```

Where the default is `--inpath data/kjvdat.txt`. However, it is ignored if the `--socket` option is given. If you don't want the program to terminate automatically after 5 seconds, use the `--no-term` option.

## How Spark Streaming Works

Spark Streaming uses a clever hack; it runs more or less the same Spark API (or code that at least looks conceptually the same) on *deltas* of data, say all the events received within one-second intervals (which is what we used here). Deltas of one second to several minutes are most common. Each delta of events is stored in its own [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) encapsulated in a [DStream](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.dstream.DStream) ("Discretized Stream").

A [StreamingContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext) is used to wrap the normal [SparkContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.SparkContext), too.

## The SparkStreaming8 Code

Here are the key parts of the code for [SparkStreaming8.scala](#code/src/main/scala/sparkworkshop/SparkStreaming8.scala):

```
...
case class EndOfStreamListener(sc: StreamingContext) extends StreamingListener {
  override def onReceiverError(error: StreamingListenerReceiverError):Unit = {
    out.println(s"Receiver Error: $error. Stopping...")
    sc.stop()
  }
  override def onReceiverStopped(stopped: StreamingListenerReceiverStopped):Unit = {
    out.println(s"Receiver Stopped: $stopped. Stopping...")
    sc.stop()
  }
}
```

The `EndOfStreamListener` will be used to detect when a socket connection drops. It will start the exit process. Note that it is not triggered when the end of file is reached while reading directories of files. In this case, we have to rely on the 5-second timeout to quit.

```
...
val conf = new SparkConf()
         .setMaster("local[2]")
         .setAppName("Spark Streaming (8)")
         .set("spark.cleaner.ttl", "60")
         .set("spark.files.overwrite", "true")
         // If you need more memory:
         // .set("spark.executor.memory", "1g")
val sc  = new SparkContext(conf)
val ssc = new StreamingContext(sc, Seconds(1))
ssc.addStreamingListener(EndOfStreamListener(ssc))
```

Construct the `SparkContext` a different way, by first defining a `SparkConf` (configuration) object. First, it is necessary to use at least 2 cores when running locally, which is specified using `setMaster("local[2]")` to avoid a [problem discussed here](http://apache-spark-user-list.1001560.n3.nabble.com/streaming-questions-td3281.html).

Spark Streaming requires the TTL to be set, `spark.cleaner.ttl`, which defaults to infinite. This specifies the duration in seconds for how long Spark should remember any metadata, such as the stages and tasks generated, etc. Periodic clean-ups are necessary for long-running streaming jobs. Note that an RDD that persists in memory for more than this duration will be cleared as well. See [Configuration](http://spark.apache.org/docs/1.0.0/configuration.html) for more details.

With the `SparkContext`, we create a `StreamingContext`, where we also specify the time interval. Finally, we add a listener for socket drops.

```
try {
  val lines =
    if (argz("socket") == "") useDirectory(ssc, argz("input-path"))
    else useSocket(ssc, argz("socket"))
```

If a socket connection wasn't specified, then use the `input-path` to read from one or more files (the default case). Otherwise use a socket. An `InputDStream` is returned in either case as `lines`. The two methods `useDirectory` and `useSocket` are listed below.

From the `DStream` (Discretized Stream) that fronts either the socket or the files, an RDD will be periodically generated for each discrete chunk of (possibly empty) data in each 1-second interval.

Now we implement an incremental word count:

```
  val words = lines.flatMap(line => line.split("""\W+"""))

  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.transform(rdd => rdd.reduceByKey(_ + _))

  wordCounts.print()  // print a few counts...

  // Generates a new, timestamped directory for each batch interval.
  if (!quiet) println(s"Writing output to: $out")
  wordCounts.saveAsTextFiles(out, "out")

  ssc.start()
  if (argz("no-term").toString == "") ssc.awaitTermination(5 * 1000)
  else  ssc.awaitTermination()

} finally {
  // Having the ssc.stop here is only needed when we use the timeout.
  out.println("+++++++++++++ Stopping! +++++++++++++")
  ssc.stop()
}
```

This works much like our previous word count logic, except for the use of `transform`, a `DStream` method for transforming the `RDDs` into new `RDDs`. In this case, we are performing "mini-word counts", within each RDD, but not across the whole `DStream`.

The `DStream` also provides the ability to do *window operations*, e.g., a moving average over the last N intervals.

Lastly, we wait for termination. We set a `timeout` of 5 seconds unless the `--no-term` option was specified.

The code ends with `useSocket` and `useDirectory`:

```
  private def useSocket(sc: StreamingContext, serverPort: String): DStream[String] = {
    try {
      // Pattern match to extract the 0th, 1st array elements after the split.
      val Array(server, port) = serverPort.split(":")
      out.println(s"Connecting to $server:$port...")
      sc.socketTextStream(server, port.toInt)
    } catch {
      case th: Throwable =>
        sc.stop()
        throw new RuntimeException(
          s"Failed to initialize host:port socket with host:port string '$serverPort':",
          th)
    }
  }

  // Hadoop text file compatible.
  private def useDirectory(sc: StreamingContext, dirName: String): DStream[String] = {
    out.println(s"Reading 'events' from directory $dirName")
    sc.textFileStream(dirName)
  }
}
```

This is just the tip of the iceberg for Streaming. See the [Streaming Programming Guide](http://spark.apache.org/docs/1.0.0/streaming-programming-guide.html) for more information.

## SparkSQL9

[SparkSQL9.scala](#code/src/main/scala/sparkworkshop/SparkSQL9.scala)

The last set of examples and exercises explores the new SparkSQL API, which extends RDDs with a "schema" for records, defined using Scala _case classes_, and allows you to embed queries using a subset of SQL in strings, as a supplement to the regular manipulation methods on the RDD type; use the best tool for the job in the same application! There is also a builder DSL for constructing these queries, rather than using SQL strings.

Furthermore, SparkSQL can read and write the new [Parquet](http://parquet.io) format that's becoming popular in Hadoop environments. SparkSQL also supports parsing JSON-formatted records, with inferred schemas, and writing RDDs in JSON.

Finally, SparkSQL embeds access to a Hive _metastore_, so you can create and delete tables, and run queries against them using Hive's query language, HiveQL.

This example treats the KJV text we've been using as a table with a schema. It runs several queries on the data.

There is a `SparkSQL9.scala` program that you can run as before, but SQL queries are more interesting when used interactively. So, there's also a "script" form called `SparkSQL9-script.scala`, which we'll look at instead. Then we'll see how to run this code and thereby learn how to use the Spark interactive shell, even in Hadoop!

The codez!

```
// Adapted from SparkSQL9, but written as a script for easy use with the
// spark-shell command.
import com.typesafe.sparkworkshop.util.Verse
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
```

The helper class `Verse` will be used to define the schema for Bible verses. Note the new imports.

Next define a convenience function for collecting an `RDD` into an array and dumping its contents to the console. Since there could be a lot of "records", we'll take the first `n`, where `n` defaults to 100.

```
def dump(rdd: RDD[_], n: Int = 100) = rdd
  .collect()        // Convert to a regular in-memory collection.
  .take(n)          // Take the first n lines.
  .foreach(println) // Print the query results.
```

Now create a [SQLContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SQLContext) that wraps the SparkContext, just like the `StreamingContext` did. We don't show this here, but in fact you can mix SparkSQL _and_ Spark Streaming in the same program.

```
val sqlc = new SQLContext(sc)
import sqlc._
```

The import statement brings SQL-specific functions and values in scope.

Next we use a regex to parse the input verses and extract the book abbreviation, chapter number, verse number, and text. The fields are separated by "|", and also removes the trailing "~" unique to this file. Then it invokes `flatMap` over the file lines (each considered a record) to extract each "good" lines and convert them into a `Verse` instances. `Verse` is defined in the `util` package. If a line is bad, a log message is written and an empty sequence is returned. Using `flatMap` and sequences means we'll effectively remove the bad lines.

We use `flatMap` over the results so that lines that fail to parse are essentially put into empty lists that will be ignored.

```
// Regex to match the fields separated by "|".
// Also strips the trailing "~" in the KJV file.
val lineRE = """^\s*([^|]+)\s*\|\s*([\d]+)\s*\|\s*([\d]+)\s*\|\s*(.*)~?\s*$""".r
// Use flatMap to effectively remove bad lines.
val verses = sc.textFile(argz("input-path").toString) flatMap {
  case lineRE(book, chapter, verse, text) =>
    Seq(Verse(book, chapter.toInt, verse.toInt, text))
  case line =>
    Console.err.println("Unexpected line: $line")
    Seq.empty[Verse]  // Will be eliminated by flattening.
}
```

Register the RDD as a temporary "table", so we can write SQL queries against it. As the name implies, this "table" only exists for the life of the process. Then we write queries and dump the results!

```
verses.registerTempTable("kjv_bible")
verses.cache()
dump(verses)  // print the 1st 100 lines

val godVerses = sql("SELECT * FROM kjv_bible WHERE text LIKE '%God%'")
println("Number of verses that mention God: "+godVerses.count())
dump(godVerses)  // print the 1st 100 lines

val counts = sql("SELECT book, COUNT(*) FROM kjv_bible GROUP BY book")
  // Collect all partitions into 1 partition. Otherwise, there are 100s
  // output from the last query!
  .coalesce(1)
dump(counts)  // print the 1st 100 lines
```

Note that the SQL dialect currently supported by the `sql` method is a subset of [HiveSQL](http://hive.apache.org). For example, it doesn't permit column aliasing, e.g., `COUNT(*) AS count`. Nor does it appear to support `WHERE` clauses in some situations.

## Using SQL in the Spark Shell

So, how do we use this script? To run it in Hadoop, you can run the script using the following script command, which wraps Spark's own `spark-shell`:

```
scripts/sparkshell.sh src/main/scala/sparkworkshop/SparkSQL9-script.scala
```

Or, start the interactive shell and then copy and past the statements one at a time to see what they do. I recommend this approach for the the first time:

```
scripts/sparkshell.sh
```

This script does some set up, but essentially its equivalent to the following:

```
$SPARK_HOME/bin/spark-shell \
  --jars target/scala-2.10/activator-spark_2.10-2.1.0.jar [arguments]
```

The jar file contains all the project's build artifacts (but not the dependencies).

To run this script locally, use the Activator shell's `console` command. Assuming you're at the shell's prompt `>`, use the following commands to enter the Scala interpreter ("REPL") and then load and run the whole file.

```
(Activator-Spark)> console
scala> :load src/main/scala/sparkworkshop/SparkSQL9-script.scala
...
scala> :quit
(Activator-Spark)> exit
```

To enter the statements using copy and paste, just paste them at the `scala>` prompt instead of loading the file.

## SparkSQLParquet10

[SparkSQLParquet10.scala](#code/src/main/scala/sparkworkshop/hadoop/SparkSQLParquet10.scala)

This script demonstrates the methods for reading and writing files in the [Parquet](http://parquet.io) format. It reads in the same data as in the previous example, writes it to new files in Parquet format, then reads it back in and runs queries on it.

> NOTE: Running this script requires a Hadoop installation, therefore it won't work in local mode, i.e., the Activator shell `console`. This is why it isn the `hadoop` package.

The key [SchemaRDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SchemaRDD) methods are `SchemaRDD.saveAsParquetFile(outpath)` and `SqlContext.parquetFile(inpath)`. The rest of the details are very similar to the previous exercise.

See the script for more details. Run it in Hadoop using the same techniques as for `SparkSQL9`.

## HiveSQL11

[HiveSQL11.scala](#code/src/main/scala/sparkworkshop/hadoop/HiveSQL11.scala)

The previous examples used the new [Catalyst](http://databricks.com/blog/2014/03/26/spark-sql-manipulating-structured-data-using-spark-2.html) query engine. However, SparkSQL also has an integration with Hive, so you can write HiveQL (HQL) queries, manipulate Hive tables, etc. This example demonstrates this feature. So, we're not using the Catalyst SQL library, but Hive's.

> NOTE: Running this script requires a Hadoop installation, therefore it won't work in local mode, i.e., the Activator shell `console`. This is why it isn the `hadoop` package.

Note that the Hive "metadata" is stored in a `megastore` directory created in the current working directory. This is written and managed by Hive's embedded [Derby SQL](http://db.apache.org/derby/) store, but it's not a production deployment option.

Let's discuss the code hightlights. There is additional imports for Hive:

```
import org.apache.spark.sql._
import org.apache.spark.sql.hive.LocalHiveContext
import com.typesafe.sparkworkshop.util.Verse
```

We need the user name.

```
val user = sys.env.get("USER") match {
  case Some(user) => user
  case None =>
    println("ERROR: USER environment variable isn't defined. Using root!")
    "root"
}
```

Create a [HiveContext](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.hive.LocalHiveContext), analogous to the previous `SQLContext`. Then define a helper function to run the query using the new `hql` function, after which we collect the result into an array and print each line.

```
val sc = new SparkContext("local[2]", "Hive SQL (10)")
val hiveContext = new LocalHiveContext(sc)
import hiveContext._   // Make methods local, like for SQLContext

def hql2(title: String, query: String, n: Int = 100): Unit = {
  println(title)
  println(s"Running query: $query")
  hql(query).collect.take(n).foreach(println)
}
```

We can now execute Hive DDL statements, such the following statements to create a database, "use it" as the working database, and then a table inside it.

```
hql2("Create a work database:", "CREATE DATABASE work")
hql2("Use the work database:", "USE work")

hql2("Create the 'external' kjv Hive table:", s"""
  CREATE EXTERNAL TABLE IF NOT EXISTS kjv (
    book    STRING,
    chapter INT,
    verse   INT,
    text    STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/user/$user/data/hive-kjv'""")
```

Here we use a triple-quoted string to specify a multi-line HiveQL statement to create a table. In this case, an `EXTERNAL` table is created, a Hive extension, where we just tell it use data in a particular directory (`LOCATION`). Here is why we needed the user name, because Hive expects an absolute path.

A few points to keep in mind:

* **Omit semicolons** at the end of the HQL (Hive SQL) string. While those would be required in Hive's own REPL or scripts, they cause errors here!
* The query results are returned in an RDD as for the other SparkSQL queries. To dump to the console, you have to use the conversion we implemented in `hql2`.

## Tip: Writing Serializable Closures

Let's end with a tip; how to write "safe" closures. When you use a closure (anonymous function), Spark will serialize it and send it around the cluster. This means that any captured variables must be serializable.

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

This is a general issue for distributed programs written for the JVM. A future version of Scala may introduce a "serialization-safe" mechanism for defining closures for this purpose.

## Going Forward from Here

To learn more, see the following resources:

* [Typesafe's Big Data Solutions](http://typesafe.com/reactive-big-data). Typesafe now offers more detailed Spark training and consulting services. Additional products and services are forthcoming.
* The Apache Spark [website](http://spark.apache.org/).
* The Apache Spark [Quick Start](http://spark.apache.org/docs/latest/quick-start.html). See also the examples in the [Spark distribution](https://github.com/apache/spark) and be sure to study the [Scaladoc](http://spark.apache.org/docs/1.0.0/api.html) pages for key types such as [RDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD) and [SchemaRDD](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.SchemaRDD).
* The [SparkSQL Programmer's Guide](http://spark.apache.org/docs/latest/sql-programming-guide.html)
* [Talks from Spark Summit 2013](http://spark-summit.org/2013).
* [Talks from Spark Summit 2014](http://spark-summit.org/2014/training).

**Experience Reports:**

* [Spark at Twitter](http://www.slideshare.net/krishflix/seattle-spark-meetup-spark-at-twitter)

**Other Spark Based Libraries:**

* [Snowplow's Spark Example Project](https://github.com/snowplow/spark-example-project).
* [Thunder - Large-scale neural data analysis with Spark](https://github.com/freeman-lab/thunder).

## For more about Typesafe:

* See [Typesafe Activator](http://typesafe.com/activator) to find other Activator templates.
* See [Typesafe Reactive Big Data](http://typesafe.com/reactive-big-data) for more information about our products and services around Big Data.
* See [Typesafe](http://typesafe.com) for information about our other products and services.

## Final Thoughts

Thank you for working through this workshop. Feedback, including pull requests for enhancements are welcome.

[Dean Wampler](mailto:dean.wampler@typesafe.com)
