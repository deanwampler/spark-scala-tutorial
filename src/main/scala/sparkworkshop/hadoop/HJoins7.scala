package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the Joins example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HJoins7 {
  def main(args: Array[String]): Unit = {
    Hadoop("Joins7", "output/kjv-joins", args)
  }
}
