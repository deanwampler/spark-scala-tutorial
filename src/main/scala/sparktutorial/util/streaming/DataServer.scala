package util.streaming
import java.io.PrintWriter
import java.nio.file.{Files, FileSystems, Path}
import java.nio.file.attribute.BasicFileAttributes
import java.util.function.BiPredicate
import scala.util.control.NonFatal
import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Serves data to the SparkStreaming example by periodically writing a new
 * file to a watched directory.
 * An alternative to invoking this code as a separate program is to invoke
 * the {@link run} method in a dedicated thread in another application.
 */
trait DataServer extends Runnable {

  val sourcePath: Path
  val iterations: Int
  val sleepIntervalMillis: Int

  import DataServer._

  def run: Unit = {
    var consumer = makeConsumer()
    try {
      val sources = getSourcePaths(sourcePath)
      for {
        count <- 1 to iterations
        source <- sources
      } {
        println(s"\nIteration $count/$iterations")
        val inputLines = getLines(source)
        consumer.setSource(source)
        inputLines.foreach(consumer.consume)
        Thread.sleep(sleepIntervalMillis)
      }
    } catch {
      case NonFatal(ex) => DataServerError("Data serving failed!", ex)
    } finally {
      consumer.close()
    }
  }

  /**
   * Get the paths for the source files.
   */
  protected def getSourcePaths(root: Path): Seq[Path] =
    Files.find(sourcePath, 5,
      new BiPredicate[Path, BasicFileAttributes]() {
        def test(path: Path, attribs: BasicFileAttributes): Boolean = attribs.isRegularFile
      }).iterator.asScala.toSeq

  /**
   * Read the source files.
   */
  protected def getLines(source: Path): Iterator[String] =
    Source.fromFile(source.toAbsolutePath.toString).getLines

  protected def makeConsumer(): Consumer
}

object DataServer {

  val defaultIterations = 100
  val defaultSleepIntervalMillis = 200 // 0.2 seconds

  case class DataServerError(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

  trait Closer {
    def close(): Unit
  }

  trait Consumer extends Closer {
    def setSource(source: Path): Unit = {
      sourceOpt = Some(source)
      if (resetOnSourceChange) resetPrintWriter()
    }
    def getSource: Option[Path] = sourceOpt

    protected var sourceOpt: Option[Path] = None
    protected var printWriterOpt: Option[PrintWriter] = None

    /** Reset when a new source is set? */
    protected def resetOnSourceChange: Boolean

    def consume(s: String): String = {
      printWriterOpt.map(_.println(s))
      s
    }

    def close(): Unit = {
      printWriterOpt.map(_.close())
      doClose()
    }
    protected def doClose(): Unit

    protected def resetPrintWriter(): Unit = {
      close()
      sourceOpt match {
        case Some(s) => printWriterOpt = Some(makePrintWriter(s))
        case None => printWriterOpt = None
      }
    }

    protected def makePrintWriter(source: Path): PrintWriter
  }

  def makePath(pathString: String) = FileSystems.getDefault().getPath(pathString)

  def makePath(rootString: String, pathString: String) = FileSystems.getDefault().getPath(rootString, pathString)

}
