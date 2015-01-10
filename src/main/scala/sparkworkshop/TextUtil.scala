
object TextUtil {
  /**
   * Extract Bible verse text, the last field in |-delimited data,
   * or return the whole line (for other data).
   * Use care when splitting the string and handling an empty
   * resulting array.
   */
  def toText(str: String): String = {
    val ary = str.toLowerCase.split("\\s*\\|\\s*")
    if (ary.length > 0) ary.last else ""
  }
}
