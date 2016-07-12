package util

import java.util.Date
import java.text.SimpleDateFormat

/**
 * Used to create timestamp strings with a format that's valid for file paths.
 */
object Timestamp {
  val fmt = new SimpleDateFormat ("yyyy.MM.dd-hh.mm.ss");

  // Simple hack for testing. Not threadsafe, of course...
  var isTest: Boolean = false

  def now(): String =
    if (isTest) ""
    else fmt.format(new Date())
}
