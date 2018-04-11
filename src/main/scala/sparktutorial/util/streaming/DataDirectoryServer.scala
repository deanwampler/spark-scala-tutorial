package util.streaming
import java.nio.file.{Files, FileSystems, Path}
import java.io.{File, PrintWriter}

/**
 * Serves data to the SparkStreaming example by periodically writing a new
 * file to a watched directory.
 * An alternative to invoking this code as a separate program is to invoke
 * the {@link run} method in a dedicated thread in another application.
 */
case class DataDirectoryServer(
  watchedDirectory: Path,
  sourcePath: Path,
  sleepIntervalMillis: Int = DataServer.defaultSleepIntervalMillis) extends DataServer {

  println(s"Starting $this")
  import DataServer.Consumer
  protected def makeConsumer(): Consumer = new DataDirectoryServer.DirectoryConsumer(watchedDirectory)
}

object DataDirectoryServer {

  import DataServer._

  class DirectoryConsumer(dirPath: Path) extends Consumer {
    Files.createDirectories(dirPath)
    if (Files.isDirectory(dirPath) == false)
      throw DataServerError(s"Given path $dirPath is not a directory")
    val dir = new File(dirPath.toString)

    /** Reset when a new source is set? */
    protected def resetOnSourceChange: Boolean = true

    protected def makePrintWriter(source: Path): PrintWriter = {
      val outFile = File.createTempFile("copy-", "txt", dir)
      new PrintWriter(outFile)
    }

    protected def doClose(): Unit = {}
  }

  /**
   * Usage: DataDirectoryServer watched_dir source_path [iterations]
   * where the iterations are the number of times to read the files in source_path
   * and write its contents to the watched_dir. The default number of iterations
   * is no limit.
   */
  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("DataDirectoryServer: ERROR - Must specify the port and source data file")
    }
    val (watchedDirectoryString, sourceString) = (args(0), args(1))
    new DataDirectoryServer(makePath(watchedDirectoryString), makePath(sourceString)).run
  }
}
