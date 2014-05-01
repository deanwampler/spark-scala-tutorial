package spark.activator

import spark.activator.util.Timestamp
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

/**
 * A demonstration of Spark Streaming with incremental Word Count.
 * Run this application in one terminal, optionally specifying "server port"
 * arguments. In another terminal use NetCat (http://netcat.sourceforge.net/) or
 * NCat that comes with NMap (http://nmap.org/download.html), which is available
 * for more platforms, to send data to this process:
 * Terminal one: sbt run-main spark.activator.SparkStreaming7 [localhost [9999]]
 * Terminal two (NetCat): nc -c -l -p 9999
 * Unfortunately, Spark Streaming does not yet provide a way to detect end of
 * input from the socket! (A feature request has been posted.) So, we have to ^C
 * the application (and sbt with it). The "-c" option to nc will cause it to 
 * terminate too; it DOES detect dropped sockets.
 */
object SparkStreaming6 {
  def main(args: Array[String]) = {

    // Use a configuration object this time, in part because Streaming requires
    // the TTL to be set:
    // spark.cleaner.ttl:  (default: infinite)
    // Duration (seconds) of how long Spark will remember any metadata
    // (stages generated, tasks generated, etc.). Periodic cleanups will
    // ensure that metadata older than this duration will be forgetten.
    // This is useful for running Spark for many hours / days (for example,
    // running 24/7 in case of Spark Streaming applications). Note that any
    // RDD that persists in memory for more than this duration will be
    // cleared as well.
    // See See http://spark.apache.org/docs/0.9.0/configuration.html for more.
    // Also see http://apache-spark-user-list.1001560.n3.nabble.com/streaming-questions-td3281.html
    // for pointers to debug a "BlockManager" problem when streaming. Specifically
    // for this example, it was necessary to specify 2 cores using 
    // setMaster("local[2]").
    val conf = new SparkConf()
             .setMaster("local[2]")
             .setAppName("Spark Streaming (7)")
             .set("spark.cleaner.ttl", "60")
             .set("spark.files.overwrite", "true")
             // If you need more memory:
             // .set("spark.executor.memory", "1g")
    val sc  = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // (A different argument list than CommandLineOptions handles.)
    // Create a DStream (Discretized Stream) that will connect to server:port
    // and periodically generate an RDD from a discrete chunk of data.
    val (server, port) = args.toList match {
      case server :: port :: _ => (server, port.toInt)
      case port :: Nil => ("localhost", port.toInt)
      case Nil => ("localhost", 9999)
    }
    println(s"Connecting to $server:$port...")
    val lines = ssc.socketTextStream(server, port)

    // Word Count...
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