/**
 * This simple driver program lets you run SparkStreaming11Main with socket input
 * in the Activator UI.
 */
object SparkStreaming11MainSocket {

  def main(args: Array[String]): Unit = {
    SparkStreaming11Main.main(args :+ "--socket" :+ "localhost:9900")
  }
}
