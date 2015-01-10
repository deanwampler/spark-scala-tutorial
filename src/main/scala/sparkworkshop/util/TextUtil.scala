package com.typesafe.sparkworkshop.util

object TextUtil {
  /**
   * Extract Bible verse text, the last field in |-delimited data,
   * or return the whole line (for other data).
   * Use care when splitting the string and handling an empty
   * resulting array.
   * @note This is actually broken, because it should not split
   * the string unless it's really one of the religious texts.
   * For example, the Shakespeare's plays have | in the text!
   */
  def toText(str: String): String = {
    val ary = str.toLowerCase.split("\\s*\\|\\s*")
    if (ary.length > 0) ary.last else ""
  }
}
