import com.typesafe.sparkworkshop.util.{CommandLineOptions, FileUtil}
import com.typesafe.sparkworkshop.util.CommandLineOptions.Opt
import com.typesafe.sparkworkshop.util.streaming._
import scala.sys.process._
import scala.language.postfixOps
import java.net.URL
import java.io.File

/**
 * The driver program for the SparkStreaming8 example. It handles the need
 * to manage a separate process to provide the source data either over a
 * socket or by periodically writing new data files to a directory.
 * Run with the --help option for details.
 */
object SparkStreaming8Main {

  val timeout = 20 * 1000   // 20 seconds

  /**
   * The source data file to write over a socket or to write repeatedly to the
   * directory specified by --inpath.
   */
  def sourceDataFile(path: String): Opt = Opt(
    name   = "source-data-file",
    value  = path,
    help   = s"-d | --data  file   The source data file used for the socket or --input direcotry of data (default: $path)",
    parser = {
      case ("-d" | "--data") +: file +: tail => (("source-data-file", file), tail)
    })

  /**
   * By default, we want to delete the watched directory and its contents for
   * the exercise, but if you specify an "--inpath directory" that you DON'T
   * want deleted, then pass "--remove-watched-dir false".
   */
  def removeWatchedDirectory(trueFalse: Boolean): Opt = Opt(
    name   = "remove-watched-dir",
    value  = trueFalse.toString,
    help   = s"--remove-watched-dir true|false  Remove the 'watched' directory (the --inpath path)?",
    parser = {
      case "--remove-watched-dir" +: tf +: tail => (("remove-watched-dir", tf), tail)
    })

  def main(args: Array[String]): Unit = {
    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      sourceDataFile("data/kjvdat.txt"),
      CommandLineOptions.inputPath("tmp/streaming-input"),
      removeWatchedDirectory(true),
      CommandLineOptions.outputPath("output/wc-streaming"),
      // For this process, use at least 2 cores!
      CommandLineOptions.master("local[*]"),
      CommandLineOptions.socket(""),  // empty default, so we know the user specified this option.
      CommandLineOptions.terminate(30),
      CommandLineOptions.quiet)

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val in     = argz("input-path")
    val out    = argz("output-path")
    val data   = argz("source-data-file")
    val socket = argz("socket")
    val rmWatchedDir = argz("remove-watched-dir").toBoolean

    // Need to remove the data argument before calling SparkStreaming8.
    def mkStreamArgs(argsSeq: Seq[String], newSeq: Vector[String]): Vector[String] =
      argsSeq match {
        case Nil => newSeq
        case ("-d" | "--data") +: file +: tail => mkStreamArgs(tail, newSeq)
        case head +: tail => mkStreamArgs(tail, newSeq :+ head)
      }
    val streamArgs = mkStreamArgs(args, Vector.empty[String]).toArray

    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    try {
      val dataThread = if (socket == "") {
        FileUtil.mkdir(in)
        new Thread(new DataDirectoryServer(in, data));
      } else {
        val port = socket.split(":").last.toInt
        new Thread(new DataSocketServer(port, data));
      }
      dataThread.start();
      SparkStreaming8.main(streamArgs)
      // When SparkStreaming8 returns, we can terminate the DataServer
      dataThread.interrupt()
    } finally {
      if (rmWatchedDir) FileUtil.rmrf(in)
    }
  }
}
