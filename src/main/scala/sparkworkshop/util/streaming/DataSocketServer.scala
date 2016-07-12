package util.streaming
import java.net.{Socket, ServerSocket}
import java.io.{File, PrintWriter}
import scala.io.Source

/**
 * Serves data over a socket for the SparkStreaming example.
 * An alternative to invoking this code as a separate program is to invoke
 * the {@link run} method in a dedicated thread in another application.
 */
class DataSocketServer(
  port: Int, dataFile: String, iterations: Int = Int.MaxValue) extends Runnable {

  import DataSocketServer._

  def run: Unit = {
    val listener = new ServerSocket(port);
    var socketOption: Option[Socket] = None
    var fileOption: Option[File] = None
    try {
      val socket = listener.accept()
      socketOption = Some(socket)
      val file = openFile(dataFile)
      fileOption = Some(file)
      val out = new PrintWriter(socket.getOutputStream(), true)
      var count = 1
      while (count < iterations) {
        val source = Source.fromFile(file)
        source.getLines.foreach(out.println)
        source.close
        Thread.sleep(sleepInterval)
        count += 1
      }
    } finally {
       listener.close();
       for (s <- socketOption) s.close();
    }
  }

  protected def openFile(path: String): File = {
    val file = new File(path)
    if (file.exists == false) {
      throw DataSocketServerError(s"Input path $path does not exist")
    }
    if (file.isFile == false) {
      throw DataSocketServerError(s"Input path $path is not a file")
    }
    file
  }
}

object DataSocketServer {

  val sleepInterval = 2000 // 2 seconds

  case class DataSocketServerError(msg: String) extends RuntimeException(msg)

  /**
   * Usage: DataSocketServer port data-file [iterations]
   * where the iterations are the number of times to read the data file
   * and write its contents to the socket. The default is no limit.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("DataSocketServer: ERROR - Must specify the port and source data file")
    }
    val (port, dataFile) = (args(0).toInt, args(1))
    val iterations = if (args.length > 2) args(2).toInt else Int.MaxValue
    new DataSocketServer(port, dataFile, iterations).run
  }
}
