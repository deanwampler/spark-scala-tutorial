import util.{CommandLineOptions, FileUtil}
import util.CommandLineOptions.Opt
import util.Matrix
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

/**
 * Use an explicitly-parallel algorithm to sum perform statistics on
 * rows in matrices.
 */
object Matrix4 {

  case class Dimensions(n: Int, m: Int)

  def main(args: Array[String]): Unit = {

    /** A function to generate an Opt for handling the matrix dimensions. */
    def dims(value: String): Opt = Opt(
      name   = "dims",
      value  = value,
      help   = s"-d | --dims  nxm     The number of rows (n) and columns (m) (default: $value)",
      parser = {
        case ("-d" | "--dims") +: nxm +: tail => (("dims", nxm), tail)
      })

    val options = CommandLineOptions(
      this.getClass.getSimpleName,
      CommandLineOptions.outputPath("output/matrix-math"),
      CommandLineOptions.master("local"),
      CommandLineOptions.quiet,
      dims("5x10"))

    val argz   = options(args.toList)
    val master = argz("master")
    val quiet  = argz("quiet").toBoolean
    val out    = argz("output-path")
    if (master.startsWith("local")) {
      if (!quiet) println(s" **** Deleting old output (if any), $out:")
      FileUtil.rmrf(out)
    }

    val dimsRE = """(\d+)\s*x\s*(\d+)""".r
    val dimensions = argz("dims") match {
      case dimsRE(n, m) => Dimensions(n.toInt, m.toInt)
      case s =>
        println(s"""Expected matrix dimensions 'NxM', but got this: $s""")
        sys.exit(1)
    }

    val name = "Matrix (4)"
    val spark = SparkSession.builder.
      master(master).
      appName(name).
      config("spark.app.id", name).   // To silence Metrics warning.
      getOrCreate()
    val sc = spark.sparkContext

    try {
      // Set up a mxn matrix of numbers.
      val matrix = Matrix(dimensions.m, dimensions.n)

      // Average rows of the matrix in parallel:
      val sums_avgs = sc.parallelize(1 to dimensions.m).map { i =>
        // Matrix indices count from 0.
        // "_ + _" is the same as "(count1, count2) => count1 + count2".
        val sum = matrix(i-1) reduce (_ + _)
        (sum, sum/dimensions.n)
      }.collect    // convert to an array

      // Make a new sequence of strings with the formatted output, then we'll
      // dump to the output location.
      // (Use fully-qualified path to Vector to avoid confusion with Spark's
      // Vector class in MLlib.)
      val outputLines = scala.collection.immutable.Vector(
        s"${dimensions.m}x${dimensions.n} Matrix:") ++
        sums_avgs.zipWithIndex.map {
          case ((sum, avg), index) =>
            f"Row #${index}%2d: Sum = ${sum}%4d, Avg = ${avg}%3d"
        }
      val output = sc.makeRDD(outputLines)  // convert back to an RDD
      if (!quiet) println(s"Writing output to: $out")
      output.saveAsTextFile(out)

    } finally {
      spark.stop()
    }

    // Exercise: Try different values of m, n.
    // Exercise: Try other statistics, like standard deviation. Are the average
    //   and standard deviation very meaningful here?
  }
}
