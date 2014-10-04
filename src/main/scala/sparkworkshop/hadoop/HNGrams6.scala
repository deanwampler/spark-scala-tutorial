package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the NGrams example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HNGrams6 {
  def main(args: Array[String]): Unit = {
    Hadoop("NGrams6", "output/ngrams", args)
  }
}
