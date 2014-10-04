package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the second implementation of Word Count. It actually uses
 * the Scala process library to run a shell script that uses the spark-submit
 * shell command!
 */
object HWordCount3 {
  def main(args: Array[String]): Unit = {
    Hadoop("WordCount3", "output/kjv-wc3", args)
  }
}
