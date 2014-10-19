/**
 * This simple driver program lets you run SparkStreaming8Main with socket input
 * in the Activator UI.
 */
object SparkStreaming8MainSocket {

  def main(args: Array[String]): Unit = {
    SparkStreaming8Main.main(args :+ "--socket" :+ "localhost:9900")
  }
}
