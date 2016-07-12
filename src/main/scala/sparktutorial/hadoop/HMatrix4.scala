package hadoop
import util.Hadoop

/**
 * Hadoop driver for the Matrix example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HMatrix4 {
  def main(args: Array[String]): Unit = {
    Hadoop("Matrix4", "output/matrix-math", args)
  }
}
