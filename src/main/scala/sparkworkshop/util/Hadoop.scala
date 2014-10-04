package com.typesafe.sparkworkshop.util
import scala.sys.process._

/**
 * Hadoop driver helper utility. It formats and runs the spark-submit command
 * indirectly by calling a common shell script.
 */
object Hadoop {
  /**
   * Hack: the default output path must be specified here, even though it's also
   * encoded in the applications. This is because we have to pass it to the
   * hadoop.sh script.
   */
  def apply(className: String, defaultOutpath: String, args: Array[String]): Unit = {
    val user = sys.env.get("USER") match {
      case Some(user) => user
      case None =>
        println("ERROR: USER environment variable isn't defined. Using root!")
        "root"
    }

    // Did the user specify an output path? Use it instead.
    val predicate = (arg: String) => arg.startsWith("-o") || arg.startsWith("--o")
    val args2 = args.dropWhile(arg => !predicate(arg))
    val outpath = if (args2.size == 0) s"/user/$user/$defaultOutpath"
      else if (args2(1).startsWith("/")) args2(1)
      else s"/user/$user/${args(2)}"

    // We don't need to remove the output argument. A redundant occurrence
    // is harmless.
    val argsString = args.mkString(" ")
    val exitCode = s"scripts/hadoop.sh --class $className --out $outpath $argsString".!
    if (exitCode != 0) sys.exit(exitCode)
  }
}
