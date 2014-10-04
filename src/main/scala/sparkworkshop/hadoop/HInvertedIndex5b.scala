package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the InvertedIndex example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HInvertedIndex5b {
  def main(args: Array[String]): Unit = {
    Hadoop("InvertedIndex5b", "output/inverted-index", args)
  }
}
