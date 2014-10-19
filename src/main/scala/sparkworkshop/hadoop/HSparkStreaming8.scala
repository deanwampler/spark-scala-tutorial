package hadoop
import com.typesafe.sparkworkshop.util.Hadoop
import sparkstreaming.ThreadStarter

/**
 * Hadoop driver for the SparkStreaming example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 * See the comments in SparkStreaming8 for how to setup another process that
 * is the source of data.
 */
object HSparkStreaming8 extends ThreadStarter {
  def main(args: Array[String]): Unit = {
    val (port, dataFile, inputPath) = parseArgs(args)
    val dataThread =
      if (port != 0) startSocketDataThread(port, dataFile)
      else startDirectoryDataThread(inputPath, dataFile)

    Hadoop("SparkStreaming8", "output/wc-streaming", args)
    // When the previous expression returns, we can terminate the data thread.
    dataThread.interrupt()
  }

  protected def parseArgs(args: Array[String]): (Int, String, String) = {
    var port = 0
    var dataFile = "data/kjvdat.txt"
    var inputPath = "tmp/streaming-input"
    def findArgs(argsSeq: Seq[String]): Unit = argsSeq match {
      case Nil => // done
      case ("-s" | "--socket") +: hostPort +: tail =>
        port = hostPort.split(":").last.toInt
        findArgs(tail)
      case ("-i" | "--in" | "--inpath") +: path +: tail =>
        inputPath = path
        findArgs(tail)
      case ("-d" | "--data") +: file +: tail =>
        dataFile = file
        findArgs(tail)
    }
    (port, dataFile, inputPath)
  }
}
