# Apache Spark: A Tutorial

## Introduction

This tutorial demonstrates how to write and run [Apache Spark](http://spark.apache.org) *Big Data* applications. 

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

We will use Spark 1.0.0-RC3 so we can use some new features, like Spark SQL, that will be generally available (GA) soon. I'll update the tutorial to final artifacts when they are available.

For now, we will use the following temporary locations for documentation and jar files:

* [Documentation](http://people.apache.org/~pwendell/spark-1.0.0-rc3-docs/).
* [Maven Repo](https://repository.apache.org/content/repositories/orgapachespark-1012/).

I recommend that you open the [Documentation link](http://people.apache.org/~pwendell/spark-1.0.0-rc3-docs/), as the getting-started guides, overviews, and reference pages (*Scaladocs*) will be useful. The Maven repo is used automatically by the build process.

## Building and Running

If you're using the [Activator UI](http://typesafe.com/activator), search for `activator-spark` and install it in the UI. The code is built automatically. 

If you grabbed this tutorial from [Github](https://github.com/deanwampler/activator-spark), you'll need to install `sbt` and use a command line to build and run the applications. In that case, see the [sbt website](http://www.scala-sbt.org/) for instructions on installing `sbt`.

If you're using the Activator UI the <a class="shortcut" href="#run">run</a> panel, select one of the bullet items under "Main Class", for example `WordCount2`, and click the "Start" button. The "Logs" panel shows some information. Note the "output" directories listed in the output. Use a file browser to find those directories to view the output written in those locations.

If you are using `sbt` from the command line, start `sbt`, then type `run`. It will present the same list of main classes. Enter one of the numbers to select the example you want to run. Try **WordCount2** to verify everything works.

Note that the examples with package names that contain `solns` are solutions to exercises. The `other` examples show alternative implementations.

Here is a list of the examples. In subsequent sections, we'll dive into the details for each one. Note that each name ends with a number, indicating the order in which we'll discuss and try them:

* **Intro1:** Actually, this *isn't* listed by the `run` command, because it is a script we'll use with the interactive *Spark Shell*.
* **WordCount2:** The *Word Count* algorithm: Read a corpus of documents, tokenize it into words, and count the occurrences of all the words. A classic, simple algorithm used to learn many Big Data APIs. By default, it uses a file containing the King James Version (KJV) of the Bible. (The `data` directory has a [REAMDE](data/README.html) that discusses the sources of the data files.)
* **WordCount3:** An alternative implementation of *Word Count* that uses a slightly different approach and also uses a library to handle input command-line arguments, demonstrating some idiomatic (but fairly advanced) Scala code.
* **Matrix4:** Demonstrates Spark's Matrix API, useful for many machine learning algorithms.
* **Crawl5a:** Simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines. The documents "crawled" are sample emails from the Enron email dataset, each of which has been classified already as SPAM or HAM.
* **InvertedIndex5b:** Using the crawl data, compute the index of words to documents (emails).
* **NGrams6:** Find all N-word ("NGram") occurrences matching a pattern. In this case, the default is the 4-word phrases in the King James Version of the Bible of the form `% love % %`, where the `%` are wild cards. In other words, all 4-grams are found with `love` as the second word. The `%` are conveniences; the NGram Phrase can also be a regular expression, e.g., `% (hat|lov)ed? % %` finds all the phrases with `love`, `loved`, `hate`, and `hated`. 
* **SparkStreaming7:** The streaming capability is relatively new and this example shows how it works to construct a simple "echo" server. Running it is a little more involved. See below.
* **SparkSQL8:** An "alpha" feature of Spark 1.0.0 is an integrated SQL query library that is based on a new query planner called Catalyst. The plan is to replace the Shark (Hive) query planner with Catalyst. For now, SparkSQL provides an API for running SQL queries over RDDs and seamless interoperation with Hive/Shark tables.
* **Shark9:** We'll briefly look at Shark, a port of the MapReduce-based [Hive](http://hive.apache.org) SQL query tool to Spark.
* **MLlib10:** One of the early uses for Spark was machine learning (ML) applications. It's not an extensive library, but it's growing fast. For example, the [Mahout](http://mahout.apache.org) project is planning to port its MapReduce algorithsm to Spark. This exercise looks at a representative ML problem.
* **GraphX11:** Our last example explores the Spark graph library GraphX.

Let's examine these examples in more detail...

## Intro1:

<a class="shortcut" href="#code/src/main/scala/spark/Intro1.sc">Intro1.sc</a>

Our first exercise demonstrates the useful *Spark Shell*, which is a customized version of Scala's REPL (read, eval, print, loop). We'll copy and paste some commands from the file <a class="shortcut" href="#code/src/main/scala/spark/Intro1.sc">Intro1.sc</a>. 

The comments in this and the subsequent files try to explain the API calls being made.  

You'll note that the extension is `.sc`, not `.scala`. This is my convention to prevent the build from compiling this file, which won't compile because it's missing some definitions that the Spark Shell will define automatically.

TODO
. $FWDIR/bin/load-spark-env.sh
        $FWDIR/bin/spark-class org.apache.spark.repl.Main "$@"
args for the shell

The shell automatically defines a `ScalaContext`, which is used to construct and run the job. `Intro1.sc`

TODO

There are comments at the end of each source file with suggested exercises to learn the API. Try them as time permits. Solutions for some of them are provided in the `src/main/scala/spark/solns` directory. I don't provide solutions for all the suggested exercises, but I do accept pull requests ;) All the solutions provided are also built with the rest of the code, so you can run them the same way.

> Note: If you don't want to modify the distribution code when working on an exercise, just copy the file and give the exercise type a new name.

## WordCount2:

<a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> 

The classic, simple *Word Count* algorithm is easy to understand and a suitable for parallel computation, so it's a good choice when learning a Big Data API.

In *Word Count*, you read a corpus of documents (you can do this in parallel), tokenize each one into words, and count the occurrences of all the words globally. By default, <a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> uses a file containing the King James Version (KJV) of the Bible. Subsequent exercises will accept command-line arguments to override such settings.

> The `data` directory has a [README](data/README.html) that discusses the files present and where they came from.

If using the <a class="shortcut" href="#run">run</a> panel, select `scala.WordCount2` and click the "Start" button. The "Logs" panel shows some information. Note the "output" directories listed in the output. Use a file browser to find those directories (which have a timestamp) to view the output written in there.

If using `sbt`, enter `run` and then the number corresponding to `scala.WordCount2`.

The reason the output directories have a timestamp in their name is so you can easily rerun the examples repeatedly. Starting with v1.0.0, Spark follows the Hadoop convention of refusing to overwrite an existing directory. The timestamps keep them unique.

There are exercises in the file and solutions for some of them, for example <a class="shortcut" href="#code/src/main/scala/spark/solns/WordCount2GroupBy.scala">solns/WordCount2GroupBy.scala</a> solves a "group by" exercise.

When you construct a `SparkContext`, the first argument specifies the "master", one of the following:

* `local`: Start Spark and use a single thread to run the job.
* `local[k]`: Use `k` threads.
* `mesos://host:port`: Connect to a running, Mesos-managed Spark cluster.
* `spark://host:port`: Connect to a running, standalone Spark cluster.

## WordCount3:

<a class="shortcut" href="#code/src/main/scala/spark/WordCount3.scala">WordCount3.scala</a> 

This example also implements *Word Count*, but it uses a slightly different approach. It also uses a library to handle input command-line arguments, demonstrating some idiomatic (but fairly advanced) Scala code.

Command line options can be used to override the defaults. You have to use `sbt` for this. Note the use of the `run-main` task that lets us specify a particular "main" to run and optional arguments. The "\" are used to wrap long lines:

```
run-main spark.WordCount3 [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

Where the options have the following meaning:

```
-h | --help     Show help and exit
-i ... input    Read this input source (default: data/kjvdat.txt)
-o ... output   Write to this output location (default: output/kjvdat-wc.txt)
-m ... master   local, local[k], etc. as discussed previously.
```

You can try different variants of `local[k]`, but keep k less than the number of cores in your machine. We'll briefly discuss the other options at the end.

When you specify an input, you can specify `bash`-style "globs" and even a list of them.

* `data/foo`: Just the file `foo` or if it's a directory, all its files, one level deep (unless the program does some extra handling itself).
* `data/foo*.txt`: All files in `data` whose names start with `foo` and end with the `.txt` extension.
* `data/foo*.txt,data2/bar*.dat`: A comma-separated list of globs.

When you specify an output location, a timestamp is appended to it, for the reasons mentioned previously.

if `RDD.saveAsTextFile(out)` is used, a Hadoop-style output *directory* is created with `part-NNNN` data files (one per process), a `_SUCCESS` file to indicate completion, and some CRC files. If the output location is used to dump regular Scala data structures, the location is interpreted as a file.

## Matrix4

<a class="shortcut" href="#code/src/main/scala/spark/Matrix4.scala">Matrix4.scala</a> 

Demonstrates Spark's Matrix API, useful for many machine learning algorithms.

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

The first part of the fifth example. Simulates a web crawler that builds an index of documents to words, the first step for computing the *inverse index* used by search engines. The documents "crawled" are sample emails from the Enron email dataset, each of which has been previously classified already as SPAM or HAM.

`Crawl5a` supports the same command-line options as `WordCount3`:

```
run-main spark.Crawl5a [ -h | --help] \ 
  [-i | --in | --inpath input] \ 
  [-o | --out | --outpath output] \ 
  [-m | --master master]
```

In this case, no timestamp is append to the output path, since it will be read by the next example `InvertedIndex5b`. So, if you rerun `Crawl5a`, you'll have to delete or rename the previous output.

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

The streaming capability is relatively new and this example shows how it works to construct a simple "echo" server. Running it is a little more involved.

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

(Not to be confused with the X11 windowing system...) Our last example explores the Spark graph library GraphX.


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

Here is an example for `NGrams`, using HDFS, not the local file system, and assuming the JobTracker host is determined from the local configuration files, so we don't have to specify it:

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

## Arguments for Spark Context

TODO 

## Other Features of the API

TODO

## Going Forward from Here

This template is not a complete Apache Spark tutorial. To learn more, see the following:

* The Apache Spark [website](http://spark.apache.org/). 
* The Apache Spark [tutorial](http://spark.apache.org/tree/develop/tutorial) distributed with the [Apache Spark](http://spark.apache.org) distribution. 
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

