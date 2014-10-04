import java.io._
import scala.io.Source

// Run in local mode and local data.
object TestUtil {

  def verifyAndClean(actualFile: String, expectedFile: String, dirToDelete: String) =
    try {
      verify(actualFile, expectedFile)
    } finally {
      rmrf(dirToDelete)
    }

  def verify(actualFile: String, expectedFile: String) = {
    val actual   = Source.fromFile(actualFile)
    val expected = Source.fromFile(expectedFile)
    (actual zip expected).zipWithIndex foreach {
      case ((a, e), i) => assert(a == e, s"$a != $e at line $i")
    }
  }

  def rmrf(root: String): Unit = rmrf(new File(root))

  def rmrf(root: File): Unit = {
    if (root.isFile) root.delete()
    else if (root.exists) {
      root.listFiles foreach rmrf
      root.delete()
    }
  }

  def rm(file: String): Unit = rm(new File(file))

  def rm(file: File): Unit =
    if (file.delete == false) throw new RuntimeException(s"Deleting $file failed!")
}