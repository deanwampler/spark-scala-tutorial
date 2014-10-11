package hadoop
import com.typesafe.sparkworkshop.util.Hadoop

/**
 * Hadoop driver for the CrawlHDFS example. It actually uses the Scala process
 * library to run a shell script that uses the spark-submit shell command!
 */
object HCrawl5aHDFS {
  def main(args: Array[String]): Unit = {
    Hadoop("Crawl5aHDFS", "output/crawl", args)
  }
}
