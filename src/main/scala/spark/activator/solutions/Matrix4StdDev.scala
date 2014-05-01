package spark.activator.solutions

import spark.activator.util.{Matrix, Timestamp}
import org.apache.spark.SparkContext

/**
 * Use an explicitly-parallel algorithm to sum perform statistics on
 * rows in matrices.
 * Adds solution for the Standard Deviation exercise.
 */
object Matrix4StdDev {

  def main(args: Array[String]) = {

    case class Dimensions(m: Int, n: Int)

    val dims = args match {
      case Array(m, n) => Dimensions(m.toInt, n.toInt)
      case Array(m)    => Dimensions(m.toInt, 10)
      case Array()     => Dimensions(5,       10)
      case _ => 
        println("""Expected optional matrix dimensions, got this: ${args.mkString(" ")}""")
        sys.exit(1)
    }

    val sc = new SparkContext("local", "Matrix (4)")

    try {
      // Set up a mxn matrix of numbers.
      val matrix = Matrix(dims.m, dims.n)

      // Average rows of a matrix in parallel:
      val sums_avgs = sc.parallelize(1 to dims.m).map { i =>
        // Matrix indices count from 0. 
        val row = matrix(i-1)
        // "_ + _" is the same as "(count1, count2) => count1 + count2".
        val sum = row reduce (_ + _) 
        val avg = sum/dims.n
        val sumsquares = row.map(x => x*x).reduce(_+_)
        val stddev = math.sqrt(1.0*sumsquares) // 1.0* => so we get a double sqrt!
        (sum, avg, stddev)
      }.collect

      println(s"${dims.m}x${dims.n} Matrix:")
      sums_avgs.zipWithIndex foreach {
        case ((sum, avg, stddev), index) => 
          println(f"Row #${index}%2d: Sum = ${sum}%4d, Avg = ${avg}%3d, Std. Dev = ${stddev}%.1f")
      }
    } finally {
      sc.stop()
    }

    // Exercise: Try different values of m, n.
    // Exercise: Try other statistics, like standard deviation. Are the average
    //   and standard deviation very meaningful here?
  }
}
