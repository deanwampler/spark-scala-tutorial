package com.typesafe.sparkworkshop

import com.typesafe.sparkworkshop.util.{CommandLineOptions, Timestamp}
import com.typesafe.sparkworkshop.util.CommandLineOptions.Opt
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
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
 * Start the NetCat/NCat process first. For NetCat (nc), use this command:
 *   nc -c -l -p 9999
 * Back in the original terminal window, run SparkStreaming8 application with
 * the following option:
 *   --socket localhost:9999
 * For example, at the SBT prompt:
 *   run-main spark.SparkStreaming8 --socket localhost:9999
 * (You can use any server and port combination you want for these two processes):
 *
 * Spark Streaming assumes long-running processes. For this example, we hard-wire
 * a maximum time of 5 seconds, but you can supress this with the "--no-term"
 * invocation option, in which case it will run until you use ^C to kill it!
 * Note that the "-c" option used with "nc" above will cause it to terminate
 * automatically if the streaming process dies.
 * Nicer clean up, e.g., when a socket connection dies, is TBD.
 */
object SparkStreaming8 {

  // Override for tests, etc.
  var out = Console.out

  val timeout = 5 * 1000   // 5 seconds

  /**
   * Use "--socket host:port" to listen for events.
   * To read "events" from a directory of files instead, e.g., for testing,
   * use the "--inpath" argument.
   */
  def socket(hostPort: String): Opt = Opt(
    name   = "socket",
    value  = hostPort,
    help   = s"-s | --socket host:port  Listen to a socket for events (default: $hostPort unless --inpath used)",
    parser = {
      case ("-s" | "--socket") +: hp +: tail => (("socket", hp), tail)
    })

  /**
   * Use "--no-term" to keep this process running "forever" (or until ^C).
   */
  def noterm(): Opt = Opt(
    name   = "no-term",
    value  = "",  // ignored
    help   = s"-n | --no | --no-term  Run forever; don't terminate after $timeout seconds)",
    parser = {
      case ("-n" | "--no" | "--no-term") +: tail => (("no-term", "true"), tail)
    })

  class EndOfStreamListener(sc: StreamingContext) extends StreamingListener {
    override def onReceiverError(error: StreamingListenerReceiverError):Unit = {
      out.println(s"Receiver Error: $error. Stopping...")
      sc.stop()
    }
    override def onReceiverStopped(stopped: StreamingListenerReceiverStopped):Unit = {
      out.println(s"Receiver Stopped: $stopped. Stopping...")
      sc.stop()
    }
  }

  def main(args: Array[String]) = {

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.inputPath("data/kjvdat.txt"),
      // We write to "out" instead of to a directory:
      // CommandLineOptions.outputPath("output/kjv-wc3"),
      // For this process, use at least 2 cores!
      CommandLineOptions.master("local[2]"),
      socket(""),  // empty default, so we know the user specified this option.
      noterm(),
      CommandLineOptions.quiet)

    val argz = options(args.toList)

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
    // See http://spark.apache.org/docs/1.1.0/configuration.html for more details.
    // Also see http://apache-spark-user-list.1001560.n3.nabble.com/streaming-questions-td3281.html
    // for pointers to debug a "BlockManager" problem when streaming. Specifically
    // for this example, it was necessary to specify 2 cores using
    // setMaster("local[2]").
    val conf = new SparkConf()
             .setMaster("local[2]")
             .setAppName("Spark Streaming (8)")
             .set("spark.cleaner.ttl", "60")
             .set("spark.files.overwrite", "true")
             // If you need more memory:
             // .set("spark.executor.memory", "1g")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.addStreamingListener(new EndOfStreamListener(ssc))

    try {

      val lines =
        if (argz("socket") == "") useDirectory(ssc, argz("input-path"))
        else useSocket(ssc, argz("socket"))

      // Word Count...
      val words = lines.flatMap(line => line.split("""\W+"""))

      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      wordCounts.print()  // print a few counts...

      // Generates a separate subdirectory for each interval!!
      // val now = Timestamp.now()
      // val out = s"output/streaming/kjv-wc-$now"
      // println(s"Writing output to: $out")
      // wordCounts.saveAsTextFiles(out, "txt")

      ssc.start()
      if (argz("no-term") == "") ssc.awaitTermination(timeout)
      else  ssc.awaitTermination()
    } finally {
      // Having the ssc.stop here is only needed when we use the timeout.
      out.println("+++++++++++++ Stopping! +++++++++++++")
      ssc.stop()
    }
  }

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