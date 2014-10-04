import java.io._
import scala.io.Source

// Run in local mode and local data.
object TestUtil {

  import com.typesafe.sparkworkshop.util.FileUtil._

  def verifyAndClean(actualFile: String, expectedFile: String, dirToDelete: String) =
    try {
      verify(actualFile, expectedFile)
    } finally {
      rmrf(dirToDelete)
    }

  // We sort the output and actual lines, which is expensive and undesirable
  // for cases where order matters, but we've observed subtle order differences
  // between platforms. Furthermore, for at least one exercise, InvertedIndex5b,
  // the elements within the lines can be different, so we sort the strings
  // Not ideal, but all this seems to be the only reasonable fix.
  def verify(actualFile: String, expectedFile: String) = {
    val actual   = Source.fromFile(actualFile).getLines.toSeq.sortBy(l => l)
    val expected = Source.fromFile(expectedFile).getLines.toSeq.sortBy(l => l)
    (actual zip expected).zipWithIndex foreach {
      case ((a, e), i) =>
        val a2 = a.sortBy(c => c)
        val e2 = e.sortBy(c => c)
        assert(a2 == e2, s"$a != $e at line $i")
    }
  }
}