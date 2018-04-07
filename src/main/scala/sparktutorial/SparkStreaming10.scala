import util.{CommandLineOptions, FileUtil}
import util.CommandLineOptions.Opt
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{
  StreamingListener, StreamingListenerReceiverError, StreamingListenerReceiverStopped}

/**
 * A demonstration of Spark Streaming with incremental Word Count.
 * For simplicity, It reads the data from the KJV Bible file, by default, when
 * you give it no arguments, e.g., when you use the SBT "run" task.
 * However, the example also supports reading text from a socket, if you specify
 * the "--socket server:port" option.
 *
 * For socket I/O, open a new terminal to run NetCat (http://netcat.sourceforge.net/)
 * or NCat that comes with NMap (http://nmap.org/download.html), which is available
 * for more platforms (e.g., Windows). Use one or the other to send data to the
 * Spark Streaming process in *this* terminal window.
 * Start the NetCat/NCat process first. For NetCat (nc), use this command,
 * where you can choose a different port if you want:
 *   nc -c -l -p 9900
 * or if you get an error that "-c" isn't a valid option, just try this:
 *   nc -l 9900
 * Back in the original terminal window, run SparkStreaming10 application with
 * the following option, using the same port:
 *   --socket localhost:9900
 * For example, at the SBT prompt:
 *   run-main SparkStreaming10 --socket localhost:9900
 * (You can use any server and port combination you want for these two processes):
 *
 * Spark Streaming assumes long-running processes. For this example, we set a
 * default termination time of 30 seconds, but you can override this with the
 * "--terminate N" option with a value of 0, in which case it will run until you use ^C to kill it!
 * Note that the "-c" option used with "nc" above will cause it to terminate
 * automatically if the streaming process dies.
 * Nicer clean up, e.g., when a socket connection dies, is TBD.
 */
object SparkStreaming10 {

  val timeout = 10         // Terminate after N seconds
  val batchSeconds = 2     // Size of batch intervals

  class EndOfStreamListener(sc: StreamingContext) extends StreamingListener {
    override def onReceiverError(error: StreamingListenerReceiverError):Unit = {
      println(s"Receiver Error: $error. Stopping...")
      sc.stop()
    }
    override def onReceiverStopped(stopped: StreamingListenerReceiverStopped):Unit = {
      println(s"Receiver Stopped: $stopped. Stopping...")
      sc.stop()
    }
  }

  def main(args: Array[String]): Unit = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("tmp/streaming-input"),
      CommandLineOptions.outputPath("output/wc-streaming"),
      // For this process, use at least 2 cores!
      CommandLineOptions.master("local[*]"),
      CommandLineOptions.socket(""),     // empty default, so we know the user specified this option.
      CommandLineOptions.terminate(timeout),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
    val term   = argz("terminate").toInt

    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    // Use a configuration object this time, in part because Streaming requires
    // the TTL to be set:
    // spark.cleaner.ttl:  (default: infinite)
    // Duration (seconds) of how long Spark will remember any metadata
    // (stages generated, tasks generated, etc.). Periodic TestUtils will
    // ensure that metadata older than this duration will be forgotten.
    // This is useful for running Spark for many hours / days (for example,
    // running 24/7 in case of Spark Streaming applications). Note that any
    // RDD that persists in memory for more than this duration will be
    // cleared as well.
    // See http://spark.apache.org/docs/latest/configuration.html for more details.
    // Also see http://apache-spark-user-list.1001560.n3.nabble.com/streaming-questions-td3281.html
    // for pointers to debug a "BlockManager" problem when streaming. Specifically
    // for this example, it was necessary to specify 2 cores using
    // setMaster("local[2]").
    val name = "Spark Streaming (10)"
    val spark = SparkSession.builder.
      master("local[*]").
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      config("spark.files.overwrite", "true").
      // If you need more memory:
      // config("spark.executor.memory", "1g").
      getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(batchSeconds))
    ssc.addStreamingListener(new EndOfStreamListener(ssc))

    try {

      val lines =
        if (argz("socket") == "") useDirectory(ssc, argz("input-path"))
        else useSocket(ssc, argz("socket"))

      // Word Count...
      val words = lines.flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))

      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      wordCounts.print()  // print a few counts...

      // Generates a separate subdirectory for each interval!!
      if (!quiet) println(s"Writing output to: $out")
      wordCounts.saveAsTextFiles(out, "out")

      ssc.start()
      if (term > 0) ssc.awaitTerminationOrTimeout(term * 1000)
      else ssc.awaitTermination()
    } finally {
      // This is a good time to look at the web console again:
      if (! quiet) {
        println("""
          |========================================================================
          |
          |    Before closing down the SparkContext, open the Spark Web Console
          |    http://localhost:4040 and browse the information about the tasks
          |    run for this example.
          |
          |    When finished, hit the <return> key to exit.
          |
          |========================================================================
          """.stripMargin)
        Console.in.read()
      }
      // Having the ssc.stop here is only needed when we use the timeout.
      println("+++++++++++++ Stopping Streaming Context! +++++++++++++")
      ssc.stop(stopSparkContext = true)
    }
  }

  private def useSocket(sc: StreamingContext, serverPort: String): DStream[String] = {
    try {
      // Pattern match to extract the 0th, 1st array elements after the split.
      val Array(server, port) = serverPort.split(":")
      println(s"Connecting to $server:$port...")
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
    println(s"Reading 'events' from directory $dirName")
    // For files that aren't plain text, see StreamingContext.fileStream.
    sc.textFileStream(dirName)
  }
}
