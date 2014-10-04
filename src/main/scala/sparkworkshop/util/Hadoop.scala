package com.typesafe.sparkworkshop.util
import scala.sys.process._

/**
 * Hadoop driver helper utility. It formats and runs the spark-submit command
 * indirectly by calling a common shell script.
 */
object Hadoop {
  def apply(className: String, outpath: String, args: Array[String]): Unit = {
    val user = sys.env.get("USER") match {
      case Some(user) => user
      case None =>
        println("ERROR: USER environment variable isn't defined. Using root!")
        "root"
    }
    val argsString = args.mkString(" ")
    val exitCode = s"scripts/hadoop.sh --class $className --out /user/$user/$outpath $argsString".!
    sys.exit(exitCode)
  }
}
