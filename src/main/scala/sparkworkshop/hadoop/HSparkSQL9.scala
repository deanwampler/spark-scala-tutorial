package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the SparkSQL example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HSparkSQL9 {
  def main(args: Array[String]): Unit = {
    Hadoop("SparkSQL9", "output/kjv-spark-sql", args)
  }
}
