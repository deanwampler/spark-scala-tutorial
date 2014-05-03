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

## WordCount2:

<a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> 

The classic, simple *Word Count* algorithm is easy to understand and a suitable for parallel computation, so it's a good choice when learning a Big Data API.

In *Word Count*, you read a corpus of documents (you can do this in parallel), tokenize each one into words, and count the occurrences of all the words globally. By default, <a class="shortcut" href="#code/src/main/scala/spark/WordCount2.scala">WordCount2.scala</a> uses a file containing the King James Version (KJV) of the Bible, but this can be overridden.

The `data` directory has a [REAMDE](data/README.html) that discusses the files present and where they came from.

If using the <a class="shortcut" href="#run">run</a> panel, select `scala.WordCount2` and click the "Start" button. The "Logs" panel shows some information. Note the "output" directories listed in the output. Use a file browser to find those directories (which have a timestamp) to view the output written in there.

If using `sbt`, enter `run` and then the number corresponding to `scala.WordCount2`.

The reason the output directories have a timestamp in their name is so you can easily rerun the examples repeatedly. Starting with v1.0.0, Spark follows the Hadoop convention of refusing to overwrite an existing directory. The timestamps keep them unique.

There are comments at the end of each source file with suggested exercises to learn the API. Try some, then look at the solutions such as <a class="shortcut" href="#code/src/main/scala/spark/solns/WordCount2GroupBy.scala">solns/WordCount2GroupBy.scala</a> 

I don't provide solutions for all the suggested exercises, but I do accept pull requests ;)

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

## NGrams6

Let's see how the *NGrams* Script works. Open <a class="shortcut" href="#code/src/main/scala/scalding/NGrams.scala">NGrams.scala</a>. 

In the Run panel, select *NGrams* from the drop-down menu to invoke this script by itself.

Here is the entire script, with the comments removed:

```
import com.twitter.scalding._

class NGrams(args : Args) extends Job(args) {
  
  val ngramsArg = args.list("ngrams").mkString(" ").toLowerCase
  val ngramsRE = ngramsArg.trim
    .replaceAll("%", """ (\\p{Alnum}+) """)
    .replaceAll("""\s+""", """\\p{Space}+""").r
  val numberOfNGrams = args.getOrElse("count", "20").toInt

  val countReverseComparator = 
    (tuple1:(String,Int), tuple2:(String,Int)) => tuple1._2 > tuple2._2
      
  val lines = TextLine(args("input"))
    .read
    .flatMap('line -> 'ngram) { 
      text: String => ngramsRE.findAllIn(text.trim.toLowerCase).toIterable 
    }
    .discard('offset, 'line)
    .groupBy('ngram) { _.size('count) }
    .groupAll { 
      _.sortWithTake[(String,Int)](
        ('ngram,'count) -> 'sorted_ngrams, numberOfNGrams)(countReverseComparator)
    }
    .debug
    .write(Tsv(args("output")))
}
```

Let's walk through this code. 

```
import com.twitter.scalding._

class NGrams(args : Args) extends Job(args) {
  ...
```

We start with the Apache Spark imports we need, then declare a class `NGrams` that subclasses a `Job` class, which provides a `main` routine and other runtime context support (such as Hadoop integration). Our class must take a list of command-line arguments, which are processed for us by Apache Spark's `Args` class. We'll use these to specify where to find input, where to write output, and handle other configuration options.

```
  ...
  val ngramsArg = args.list("ngrams").mkString(" ").toLowerCase
  val ngramsRE = ngramsArg.trim
    .replaceAll("%", """ (\\p{Alnum}+) """)
    .replaceAll("""\s+""", """\\p{Space}+""").r
  val numberOfNGrams = args.getOrElse("count", "20").toInt
  ...
```

Before we create our *dataflow*, a series of *pipes* that provide data processing, we define a values that we'll need. The user specifies the NGram pattern they want, such as the "% love % %" used in our *run* example. The `ngramsRE` takes that NGram specification and turns it into a regular expression that we need. The "%" are converted into patterns to find any word and any runs of whitespace are generalized for all whitespace. Finally, we get the command line argument for the number of most frequently occurring NGrams to find, which defaults to 20 if not specified.

```
  ...
  val countReverseComparator = 
    (tuple1:(String,Int), tuple2:(String,Int)) => tuple1._2 > tuple2._2
  ...
```

The `countReverseComparator` function will be used to rank our found NGrams by frequency of occurrence, descending. The count of occurrences will be the second field in each tuple.


```
  ...
  val lines = TextLine(args("input"))
    .read
    .flatMap('line -> 'ngram) { 
      text: String => ngramsRE.findAllIn(text.trim.toLowerCase).toIterable 
    }
    .discard('offset, 'line)
    ...
```

Now our dataflow is created. A `TextLine` object is used to read each "record", a line of text as a single "field". Hence, the records are newline (`\n`) separated. It reads the file specified by the `--input` argument (processed by the `args` object). 

For input, we use a file containing the King James Version of the Bible. We have included that file; see the `data/README` file for more information.

Each line of the input actually has the following *schema*:

```
Abbreviated name of the book of the Bible (e.g., Gen) | chapter | verse | text
```

For example, this the very first (famous) line:

```
Gen|1|1| In the beginning God created the heaven and the earth.
```

Note that a flaw with our implementation is that NGrams across line boundaries won't be found, because we process each line separately. However, the text for the King James Version of Bible that we are using has each verse on its own line. It wouldn't make much sense to compute NGrams across verses, so this limitation is not an issue for this particular data set.

Next, we call `flatMap` on each line record, converting it to zero or more output records, one per NGram found. Of course, some lines won't have a matching NGram. We use our regular expression to tokenize each line, and also trim leading and trailing whitespace and convert to lower case. 

A scalding API convention is to use the first argument list to a function to specify the field names to input to the function and name the new fields output. In this case, we input just the line field, named `'line` (a Scala *symbol*) and name each found NGram `'ngram`. Note who these field names are specified using a tuple.

Finally in this section, we discard the fields we no longer need. Operations like `flatMap` and `map` append the new fields to the existing fields. We no longer need the `'line` and `TextLine` also added a line number field to the input, named `'offset`. 

```
    ...
    .groupBy('ngram) { _.size('count) }
    .groupAll { 
      _.sortWithTake[(String,Int)](
        ('ngram,'count) -> 'sorted_ngrams, numberOfNGrams)(countReverseComparator)
    }
    ...
}

```

If we want to rank the found NGrams by their frequencies, we need to get all occurrences of a given NGram together. Hence, we use a `groupBy` operation to group over the `'ngram` fields. To sort and output the tope `numberOfNGrams`, we group *all* together, then use a special Apache Spark function that combines sorting with "taking", i.e., just keeping the top N values after sorting.


```
    ...
    .debug
    .write(Tsv(args("output")))
}
```

The `debug` function dumps the current stream of data to the console, which is useful for debugging. Don't do this for massive data sets!!

Finally, we write the results as tab-separated values to the location specified by the `--output` command-line argument.

To recap, look again at the whole listing above. It's not very big! For what it does and compared to typical code bases you might work with, this is incredibly concise and powerful code.

*WordCount* is next...

## WordCount

Open <a class="shortcut" href="#code/src/main/scala/scalding/WordCount.scala">WordCount.scala</a>, which implements the well-known *Word Count* algorithm, which is popular as an easy-to-implement, "hello world!" program for developers learning Hadoop.

In *WordCount*, a corpus of documents is read, the contents are tokenized into words, and the total count for each word over the entire corpus is computed. The output is sorted by frequency descending.

In the Run panel, select *WordCount* from the drop-down menu to invoke this script by itself.

Here is the script without comments:

```
import com.twitter.scalding._

class WordCount(args : Args) extends Job(args) {

  val tokenizerRegex = """\W+"""
  
  TextLine(args("input"))
    .read
    .flatMap('line -> 'word) {
      line : String => line.trim.toLowerCase.split(tokenizerRegex) 
    }
    .groupBy('word){ group => group.size('count) }
    .write(Tsv(args("output")))
}
```

Each line is read as plain text from the input location specified by the `--input` argument, just as we did for *NGrams*. 

Next, `flatMap` is used to tokenize the line into words, similar to the first few steps in *NGrams*.

Next, we group over the words, to get all occurrences of each word gathered together, and we compute the size of each group, naming this size field `'count'. 

Finally, we write the output `'word` and `'count` fields as tab-separated values to the location specified with the `--output` argument, as for *NGrams*.

*FilterUniqueCountLimit* is next...

## FilterUniqueCountLimit

Open <a class="shortcut" href="#code/src/main/scala/scalding/FilterUniqueCountLimit.scala">FilterUniqueCountLimit.scala</a>, which shows a few useful techniques:

1. How to split a data stream into several flows, each for a specific calculation.
2. How to filter records (like SQL's `WHERE` clause).
3. How to find unique values (like SQL's `DISTINCT` keyword).
4. How to count all records (like SQL's `COUNT(*)` clause).
5. How to limit output (like SQL's `LIMIT n` clause).

In the Run panel, select *FilterUniqueCountLimit* from the drop-down menu to invoke this script by itself.

Here is the full script without comments:

```
import com.twitter.scalding._

class FilterUniqueCountLimit(args : Args) extends Job(args) {

  val kjvSchema = ('book, 'chapter, 'verse, 'text)
  val outputPrefix = args("output")

  val bible = Csv(args("input"), separator = "|", fields = kjvSchema)
      .read

  new RichPipe(bible)
      .filter('text) { t:String       => t.contains("miracle") == false }
      .write(Csv(s"$outputPrefix-skeptic.txt", separator = "|"))

  new RichPipe(bible)
      .project('book)
      .unique('book)
      .write(Tsv(s"$outputPrefix-books.txt"))  

  new RichPipe(bible)
      .groupAll { _.size('countstar).reducers(2) }
      .write(Tsv(s"$outputPrefix-count-star.txt"))  

  new RichPipe(bible)
      .limit(args.getOrElse("n", "10").toInt)
      .write(Csv(s"$outputPrefix-limit-N.txt", separator = "|"))
}
```

This time, we read each line ("record") of text as a "|"-separated fields with the fields named by the `kjvSchema` value. Each input line is a verse in the Bible. We also treat the `--output` argument as a prefix, because four separate files will be output this time.

We open the KJV file using a comma-separated values reader, but overriding the separator to be "|" and applying the `kjvSchema` specification to each record.

Now we clone this input pipe four times to do four separated operations on the data. The first pipe filters each line, removing those with the word *miracle*, thus creating a "skeptics Bible". (Thomas Jefferson could have used this feature...) The output is written to file with the name suffix `-skeptic.txt`.

The second pipe projects just the first column/field, the name of the book of the Bible and finds all the unique values for this field, thereby producing a list of books in the Bible.

The third pipe uses the `groupAll` idiom to collect all records together and count them, yielding the total number of verses in the KJV Bible, 31102.

The fourth and final pipe limits the number of records to the input value given for the `--n` argument or 10 if the argument isn't specified. Hence, it's output is just the first n lines of the KJV.

*TfIdf* is our last example script...

## TfIdf

Open <a class="shortcut" href="#code/src/main/scala/scalding/TfIdf.scala">TfIdf.scala</a>, our most complex example script. It implements the *term frequency-inverse document frequency* algorithm used as part of  the indexing process for document or Internet search engines. (See [this Wikipedia page](http://en.wikipedia.org/wiki/Tf*idf) for more information on this algorithm.)

In the Run panel, select *TfIdf* from the drop-down menu to invoke this script by itself.

In a conventional implementation of Tf-Idf, you might load a precomputed document to word matrix: 

```
a[i,j] = frequency of the word j in the document with index i 
```

Then, you would compute the Tf-Idf score of each word with respect to each document.

Instead, we'll compute this matrix by first performing a modified *Word Count* on our KJV Bible data, then convert that data to a matrix and proceed from there. The modified *Word Count* will track the source Bible book and `groupBy` the `('book, 'word)` instead of just the `'word`.

Here is the entire script without comments:

```
import com.twitter.scalding._
import com.twitter.scalding.mathematics.Matrix

class TfIdf(args : Args) extends Job(args) {
  
  val n = args.getOrElse("n", "100").toInt 
  val kjvSchema = ('book, 'chapter, 'verse, 'text)
  val tokenizerRegex = """\W+"""
  
  val books = Vector(
    "Act", "Amo", "Ch1", "Ch2", "Co1", "Co2", "Col", "Dan", "Deu", 
    "Ecc", "Eph", "Est", "Exo", "Eze", "Ezr", "Gal", "Gen", "Hab", 
    "Hag", "Heb", "Hos", "Isa", "Jam", "Jde", "Jdg", "Jer", "Jo1", 
    "Jo2", "Jo3", "Job", "Joe", "Joh", "Jon", "Jos", "Kg1", "Kg2", 
    "Lam", "Lev", "Luk", "Mal", "Mar", "Mat", "Mic", "Nah", "Neh", 
    "Num", "Oba", "Pe1", "Pe2", "Phi", "Plm", "Pro", "Psa", "Rev", 
    "Rom", "Rut", "Sa1", "Sa2", "Sol", "Th1", "Th2", "Ti1", "Ti2",
    "Tit", "Zac", "Zep")
  
  val booksToIndex = books.zipWithIndex.toMap
  
  val byBookWordCount = Csv(args("input"), separator = "|", fields = kjvSchema)
    .read
    .flatMap('text -> 'word) {
      line : String => line.trim.toLowerCase.split(tokenizerRegex) 
    }
    .project('book, 'word)
    .map('book -> 'bookId)((book: String) => booksToIndex(book))
    .groupBy(('bookId, 'word)){ group => group.size('count) }

  import Matrix._

  val docSchema = ('bookId, 'word, 'count)

  val docWordMatrix = byBookWordCount
    .toMatrix[Long,String,Double](docSchema)

  val docFreq = docWordMatrix.sumRowVectors

  val invDocFreqVct = 
    docFreq.toMatrix(1).rowL1Normalize.mapValues( x => log2(1/x) )

  val invDocFreqMat = 
    docWordMatrix.zip(invDocFreqVct.getRow(1)).mapValues(_._2)

  val out1 = docWordMatrix.hProd(invDocFreqMat).topRowElems(n)
    .pipeAs(('bookId, 'word, 'frequency))
    .mapTo(('bookId, 'word, 'frequency) -> ('book, 'word, 'frequency)){
      tri: (Int,String,Double) => (books(tri._1), tri._2, tri._3)
    }

  val abbrevToNameFile = args.getOrElse("abbrevs-to-names", "data/abbrevs-to-names.tsv")
  val abbrevToName = Tsv(abbrevToNameFile, fields = ('abbrev, 'name)).read

  out1.joinWithTiny('book -> 'abbrev, abbrevToName)
    .project('name, 'word, 'frequency)
    .write(Tsv(args("output")))

  def log2(x : Double) = scala.math.log(x)/scala.math.log(2.0)
}

```

This example uses Apache Spark's Matrix API, which simplifies working with "sparse" matrices.

Here, the `--n` argument is used to specify how many of the most frequently-occurring terms to keep for each book of the Bible. It defaults to 100.

We use the same input schema and word-tokenization we used previously for *FilterUniqueCountLimit* and *WordCount*, respectively.

We'll need to convert the Bible book names to numeric ids. We could actually compute the unique books and assign each an id (as discussed for *FilterUniqueCountLimit*), but to simplify things, we'll simply hard-code the abbreviated names used in the KJV text file and then zip this collection with the corresponding indices to create ids.

The `byBookWordCount` pipeline is very similar to *WordCount*, but we don't forget which book the word came from, so our key for grouping is now the `'booked` and the `'word`.

Next, we convert `byBookWordCount` to a term frequency, two-dimensional matrix, using Apache Spark Matrix API, where the "x" coordinate is the book id, the "y" coordinate is the word, and the value is the count, converted to `Double`.

Now we compute the overall document frequency of each word. The value of `docFreq(i)` will be the total count for word `i` over all documents. Then we need the inverse document frequency vector, which is used to suppress the significance of really common words, like "the", "and", "I", etc. The *L1 norm* is just `1/(|a| + |b| + ...)`, rather then the square root of the sum of squares, which would be more accurate, but also more expensive to compute. Actually, we use `1/log(x)`, rather than `1/x`, for better numerical stability. 

Then we zip the row vector along the entire document-word matrix and multiply the term frequency with the inverse document frequency, keeping the top N words. The value `hProd` is the *Hadamard product*, which is nothing more than multiplying matrices element-wise, rather than the usual matrix multiplication of row vector x column vector.

Before writing the output, we convert the matrix back to a Cascading pipe and replace the bookId with the abbreviated book name that we started with. Note that `mapTo` used here is like `map`, but the later keeps all the original input fields and adds the new fields created by the `map` function. In contrast, `mapTo` tosses all the original fields that aren't explicitly passed to the anonymous function. Hence, it's equivalent to the `map(...).project(...)` sequence, but more efficient by eliminating the extra intermediate step.

Finally, before writing the output, we see how joins work, which we use to bring in a table of the book abbreviations and the corresponding full names. We would rather write the full names. Otherwise, this join isn't necessary. 

This mapping is in the file `data/abbrevs-to-names.tsv`. Almost a third of the books are actually aprocryphal and hence aren't in the KJV. We'll effectively ignore those.

Note the word `Tiny` in the inner join, `joinWithTiny`. The "tiny" pipe is the abbreviations data set on the "right-hand side. Knowing which pipe has the smallest data set is important, because Cascading can use a optimization known as a *map-side join*. In short, if the small data set can fit in memory, that data can be cached in the underlying Hadoop "map" process' memory and the join can be performed as the larger pipe of data streams through. For more details on this optimization, see [this description for Hive's version of map-side joins](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+JoinOptimization#LanguageManualJoinOptimization-PriorSupportforMAPJOIN).

The pair tuple passed to `joinWithTiny` lists one or more fields from the left-hand table and a corresponding number of fields from the right-hand table to join on. Here, we just join on a single field from each pipe. If it were two, we would pass an argument like `(('l1, l2') -> ('r1, 'r2))`. Note the nested tuples within the outer pair tuple.

After joining, we do a final projection and then write the output as before.

## Next Steps

There are additional capabilities built into this Activator template that you can access from a command line using the `activator` command or [sbt](http://www.scala-sbt.org/), the standard Scala build tool. Let's explore those features.

## Running Locally with the Command Line

In a command window, run `activator` (or `sbt`). At the prompt, invoke the `test` and `run` tasks, which are the same tasks we ran in Activator, to ensure they complete successfully. 

All four examples take command-line arguments to customize their behavior. The *NGrams* example is particular interesting to play with, where you search for different phrases in the Bible or some other text file of interest to you. 

At the `activator` prompt, type `scalding`. You'll see the following:

```
> scalding
[error] Please specify one of the following commands (example arguments shown):
[error]   scalding FilterUniqueCountLimit --input data/kjvdat.txt --output output/kjv
[error]   scalding NGrams --count 20 --ngrams "I love % %" --input data/kjvdat.txt --output output/kjv-ngrams.txt
[error]   scalding TfIdf --n 100 --input data/kjvdat.txt --output output/kjv-tfidf.txt
[error]   scalding WordCount --input data/kjvdat.txt --output output/kjv-wc.txt
[error] scalding requires arguments.
```

Hence, without providing any arguments, the `scalding` command tells you which scripts are available and the arguments they support with examples that will run with supplied data in the `data` directory. Note that some of the options shown are optional (but which ones isn't indicated; see the comments in the script files). The scripts are listed alphabetically, not in the order we discussed them previously.

Each command should run without error and the output will be written to the file indicated by the `--output` option. You can change the output location to be anything you want.

Also, if you want to understand how Cascading converts each script into a MapReduce dataflow, Cascading has a feature that generates graphs of the dataflow in the [dot](http://en.wikipedia.org/wiki/DOT_(graph_description_language)) graph format. You can view these files with many tools, including the free [Graphviz](http://www.graphviz.org/).

To generate these files, you can add the argument `--tool.graph` to the end of any script command we discuss in subsequent sections. Instead of running the job, the Cascading runtime will write one or more "dot" files to the project root directory.

Now let's look at each example. Recall that all the scripts are in `src/main/scala/scalding`. You should look at the files for detailed comments on how they are implemented.

## NGrams

You invoke the *NGrams* script inside `activator` like this:

```
scalding NGrams --count 20 --ngrams "I love % %" --input data/kjvdat.txt --output output/kjv-ngrams.txt
```

The `--ngrams` phrase allows optional "context" words, like the "I love" prefix shown here, followed by two words, indicated by the two "%". Hence, you specify the desired `N` implicitly through the number of "%" placeholders and hard-coded words (4-grams, in this example). 

For example, the phrase "% love %" will find all 3-grams with the word "love" in the middle, and so forth, while the phrase "% % %" will find all 3-grams, period (i.e., without any "context").

The NGram phrase is translated to a regular expression that also replaces the whitespace with a regular expression for arbitrary whitespace.

**NOTE:** In fact, additional regular expression constructs can be used in this string, e.g., `loves?` will match `love` and `loves`. This can be useful or confusing...

The `--count n` flag means "show the top n most frequent matching NGrams". If not specified, it defaults to 20.

Try different NGram phrases and values of count. Try different data sources.

This example also uses the `debug` pipe to dump output to the console. In this case, you'll see the same output that gets written to the output file, which is the list of the NGrams and their frequencies, sorted by frequency descending.

## WordCount

You invoke the script inside `activator` like this:

```
scalding WordCount --input data/kjvdat.txt --output output/kjv-wc.txt
```

Recall that each line of the input actually has the "schema":

```
Abbreviated name of the book of the Bible (e.g., Gen) | chapter | verse | text
```

For example,

```
Gen|1|1| In the beginning God created the heaven and the earth.~
```

We just treat the whole line as text. A nice exercise is to *project* out just the `text` field. See the other scripts for examples of how to do this.

The `--output` argument specifies where the results are written. You just see a few log messages written to the `activator` console. You can use any path you want for this output.

## FilterUniqueCountLimit

You invoke the script inside `activator` like this:

```
scalding FilterUniqueCountLimit --input data/kjvdat.txt --output output/kjv
```

In this case, the `--output` is actually used as a prefix for the four output files discussed previously.

## TfIDf
 
You invoke the script inside `activator` like this:

```
scalding TfIdf --n 100 --input data/kjvdat.txt --output output/kjv-tfidf.txt
````

The `--n` argument is optional; it defaults to 100. It specifies how many words to keep for each document. 


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

### Arguments for Spark Context


## Going Forward from Here

This template is not a complete Apache Spark tutorial. To learn more, see the following:

* The Apache Spark [website](http://spark.apache.org/). 
* The Apache Spark [tutorial](http://spark.apache.org/tree/develop/tutorial) distributed with the [Apache Spark](http://spark.apache.org) distribution. 
* [Talks from Spark Summit 2013](http://spark-summit.org/2013).
* [Running Spark in EC2](http://aws.amazon.com/articles/4926593393724923).
* [Snowplow's Spark Example Project](https://github.com/snowplow/spark-example-project).
* [Thunder - Large-scale neural data analysis with Spark](https://github.com/freeman-lab/thunder).

For more about Typesafe:

* See [Typesafe Activator](http://typesafe.com/activator) to find other Activator templates.
* See [Typesafe](http://typesafe.com) for more information about our products and services. 

