import com.typesafe.sparkworkshop.util.Matrix
import org.apache.spark.SparkContext

/**
 * Use an explicitly-parallel algorithm to sum perform statistics on
 * rows in matrices.
 */
object Matrix4 {

  // Override for tests.
  var out = Console.out

  def main(args: Array[String]) = {

    case class Dimensions(m: Int, n: Int)

    // Process command-line args. differently.
    val (master, index) = if (args(0) == "--master") {
      (args(1), 2)
    } else {
      ("local", 0)
    }
    println(s"Using master: $master")

    val dims = args.drop(index) match {
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

      // Average rows of the matrix in parallel:
      val sums_avgs = sc.parallelize(1 to dims.m).map { i =>
        // Matrix indices count from 0.
        // "_ + _" is the same as "(count1, count2) => count1 + count2".
        val sum = matrix(i-1) reduce (_ + _)
        (sum, sum/dims.n)
      }.collect
      // }.coalesce    // Reduce to one partition in preparation for output.

      out.println(s"${dims.m}x${dims.n} Matrix:")
      sums_avgs.zipWithIndex foreach {
        case ((sum, avg), index) =>
          out.println(f"Row #${index}%2d: Sum = ${sum}%4d, Avg = ${avg}%3d")
      }
    } finally {
      sc.stop()
    }

    // Exercise: Try different values of m, n.
    // Exercise: Try other statistics, like standard deviation.
    // Exercise: Try other statistics, like standard deviation. Are the average
    //   and standard deviation very meaningful here?
  }
}
