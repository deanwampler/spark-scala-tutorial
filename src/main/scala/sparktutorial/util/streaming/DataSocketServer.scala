package util.streaming
import java.net.{Socket, ServerSocket}
import java.io.PrintWriter
import java.nio.file.{FileSystems, Path}

/**
 * Serves data over a socket for the SparkStreaming example.
 * An alternative to invoking this code as a separate program is to invoke
 * the {@link run} method in a dedicated thread in another application.
 */
case class DataSocketServer(
  port: Int,
  sourcePath: Path,
  iterations: Int = DataServer.defaultIterations,
  sleepIntervalMillis: Int = DataServer.defaultSleepIntervalMillis) extends DataServer {

  import DataServer.Consumer
  protected def makeConsumer(): Consumer = new DataSocketServer.SocketConsumer(port)
}

object DataSocketServer {

  import DataServer._

  class SocketConsumer(port: Int) extends Consumer {

    /** Reset when a new source is set? Not for sockets! */
    protected def resetOnSourceChange: Boolean = false

    protected var socketOption: Option[Socket] = None
    protected var listenerOption: Option[ServerSocket] = None

    protected def makePrintWriter(source: Path): PrintWriter = {
      val listener = new ServerSocket(port)
      listenerOption = Some(listener)
      val socket = listener.accept()
      socketOption = Some(socket)
      new PrintWriter(socket.getOutputStream(), true)
    }

    protected def doClose(): Unit = {
      listenerOption.map(_.close())
      socketOption.map(_.close())
    }
  }

  /**
   * Usage: DataSocketServer port source_path [iterations]
   * where the iterations are the number of times to read the files in source_path
   * and write its contents to the socket. The default number of iterations
   * is no limit.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("DataSocketServer: ERROR - Must specify the port and source data file")
    }
    val (port, sourceString) = (args(0).toInt, args(1))
    val iterations = if (args.length > 2) args(2).toInt else defaultIterations
    new DataSocketServer(port, makePath(sourceString), iterations).run
  }
}
