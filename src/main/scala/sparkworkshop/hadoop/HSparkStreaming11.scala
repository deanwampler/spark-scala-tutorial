package hadoop
import util.Hadoop
import sparkstreaming.ThreadStarter

/**
 * Hadoop driver for the SparkStreaming example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 * See the comments in SparkStreaming11 for how to setup another process that
 * is the source of data.
 */
object HSparkStreaming11 extends ThreadStarter {
  def main(args: Array[String]): Unit = {
    val (port, dataFile) = parseArgs(args)
    val dataThread = startSocketDataThread(port, dataFile)

    Hadoop("SparkStreaming11", "output/wc-streaming", args)
    // When the previous expression returns, we can terminate the data thread.
    dataThread.interrupt()
  }

  protected def parseArgs(args: Array[String]): (Int, String) = {
    var port = 0
    var dataFile = "data/kjvdat.txt"
    def findArgs(argsSeq: Seq[String]): Unit = argsSeq match {
      case Nil => // done
      case ("-s" | "--socket") +: hostPort +: tail =>
        port = hostPort.split(":").last.toInt
        findArgs(tail)
      case ("-i" | "--in" | "--inpath") +: path +: tail =>
        println("HSparkStreaming11: ERROR - Reading data from directories isn't implemented for Hadoop.")
        println("Use the --socket option instead.")
        sys.exit(1)
      case ("-d" | "--data") +: file +: tail =>
        dataFile = file
        findArgs(tail)
    }
    if (port == 0) port = 9900
    (port, dataFile)
  }
}
