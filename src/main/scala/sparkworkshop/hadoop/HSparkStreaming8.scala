package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the SparkStreaming example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 * See the comments in SparkStreaming8 for how to setup another process that
 * is the source of data.
 */
object HSparkStreaming8 {
  def main(args: Array[String]): Unit = {
    Hadoop("SparkStreaming8", "output/kjv-wc-streaming", args)
  }
}
