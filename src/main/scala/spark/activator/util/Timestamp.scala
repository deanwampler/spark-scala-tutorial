package spark.activator.util

import java.util.Date
import java.text.SimpleDateFormat

object Timestamp {
  val fmt = new SimpleDateFormat ("yyyy.MM.dd-hh.mm.ss");

  def now(): String = {
    val now = new Date()
    fmt.format(now)
  }
}
