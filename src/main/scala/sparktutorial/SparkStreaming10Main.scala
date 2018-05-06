import util.{CommandLineOptions, FileUtil}
import util.CommandLineOptions.Opt
import util.streaming._
import scala.sys.process._
import scala.language.postfixOps
import java.net.URL
import java.io.File
import java.nio.file.Path

/**
 * The driver program for the SparkStreaming10 example. It handles the need
 * to manage a separate process to provide the source data either over a
 * socket or by periodically writing new data files to a directory.
 * Run with the --help option for details.
 */
object SparkStreaming10Main {

  val iterations = 200   // Terminate after N iterations

  /**
   * The source data to write over a socket or to write repeatedly to the
   * directory specified by --inpath.
   */
  def sourceData(path: String): Opt = Opt(
    name   = "source-data",
    value  = path,
    help   = s"-d | --data  file_or_dir The source data file or directory tree used for the socket or --input direcotry of data (default: $path)",
    parser = {
      case ("-d" | "--data") +: path2 +: tail => (("source-data", path2), tail)
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

  def useSQL(trueFalse: Boolean): Opt = Opt(
    name   = "use-sql",
    value  = trueFalse.toString,
    help   = s"--sql  Use the SparkSQL stream processing example?",
    parser = {
      case "--sql" +: tail => (("use-sql", "true"), tail)
    })

  def main(args: Array[String]): Unit = {

    val defaultDuration = 30 // seconds

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      sourceData("data/enron-spam-ham"),
      CommandLineOptions.inputPath("tmp/streaming-input"),
      removeWatchedDirectory(true),
      useSQL(false),
      CommandLineOptions.outputPath("output/wc-streaming"),
      // For this process, use at least 2 cores!
      CommandLineOptions.master("local[2]"),
      CommandLineOptions.socket(""),  // empty default, so we know the user specified this option.
      CommandLineOptions.terminate(defaultDuration),
      CommandLineOptions.quiet)

    val argz         = options(args.toList)
    val master       = argz("master")
    val quiet        = argz("quiet").toBoolean
    val in           = argz("input-path")
    val out          = argz("output-path")
    val data         = argz("source-data")
    val socket       = argz("socket")
    val rmWatchedDir = argz("remove-watched-dir").toBoolean
    val seconds      = argz("terminate").toInt

    val streamArgs: Array[String] = Array(
      "--master",      master,
      "--inpath",      in,
      "--outpath",     out,
      "--socket",      socket,
      "--terminate",   seconds.toString)

    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    try {
      val dataThread =
        if (socket == "") {
          startDirectoryDataThread(in, data)
        } else {
          val port = socket.split(":").last.toInt
          startSocketDataThread(port, data)
        }
      dataThread.start()
      if (argz("use-sql").toBoolean) {
        SparkStreaming10SQL.main(streamArgs)
      } else {
        SparkStreaming10.main(streamArgs)
      }
      // When SparkStreaming10.main returns, we can terminate the data server thread:
      dataThread.interrupt()
    } finally {
      if (rmWatchedDir) FileUtil.rmrf(in)
    }
  }

  import DataServer._

  def startSocketDataThread(port: Int, dataSource: String): Thread =
    new Thread(new DataSocketServer(port, makePath(dataSource)))

  def startDirectoryDataThread(watchedDir: String, dataSource: String): Thread = {
    FileUtil.mkdir(watchedDir)
    new Thread(new DataDirectoryServer(makePath(watchedDir), makePath(dataSource)))
  }
}
