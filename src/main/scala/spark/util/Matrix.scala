package spark.activator.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** 
 * A special-purpose matrix case class. Each cell is given the value
 * i*N + j for indices (i,j), counting from 0.
 * @note: Must be serializable, which is automatic for case classes.
 */
case class Matrix(m: Int, n: Int) {
  private def makeRow(start: Long): Array[Long] = 
    Array.iterate(start, n)(i => i+1)

  private val repr: Array[Array[Long]] = 
    Array.iterate(makeRow(0), m)(rowi => makeRow(rowi(0) + n))

  /** Return row i, <em>indexed from 0</em>. */
  def apply(i: Int): Array[Long]  = repr(i)

  /** Return the (i,j) element, <em>indexed from 0</em>. */
  def apply(i: Int, j: Int): Long = repr(i)(j)

  private val cellFormat = {
    val maxEntryLength = (m*n - 1).toString.length
    s"%${maxEntryLength}d"
  }

  private def rowString(rowI: Array[Long]) = 
    rowI map (cell => cellFormat.format(cell)) mkString ", "
  override def toString = repr map rowString mkString "\n"
}
