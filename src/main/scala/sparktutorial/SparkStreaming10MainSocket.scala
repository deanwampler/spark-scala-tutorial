/**
 * This convenient driver program lets you run SparkStreaming10Main
 * with socket input.
 */
object SparkStreaming10MainSocket {

  def main(args: Array[String]): Unit = {
    SparkStreaming10Main.main(args :+ "--socket" :+ "localhost:9900")
  }
}
