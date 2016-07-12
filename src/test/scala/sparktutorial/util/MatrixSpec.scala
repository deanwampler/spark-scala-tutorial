package util
import org.scalatest.FunSpec

class MatrixSpec extends FunSpec {

  describe ("Matrix") {
    describe ("construction") {
      it ("requires positive row and column counts") {
        intercept[java.lang.AssertionError] { Matrix(0,1) }
        intercept[java.lang.AssertionError] { Matrix(1,0) }
      }

      it ("constructs an MxN matrix") {
        val m = Matrix(3,5)
        assert(m.toString == """ 0,  1,  2,  3,  4
          | 5,  6,  7,  8,  9
          |10, 11, 12, 13, 14""".stripMargin)
      }

      it ("The cell at index (m,n) has the value (m*N+n) (zero-based indexing)") {
        check(3, 5)
      }
    }

    describe ("apply(i)") {
      it ("returns row i (zero-based indexing)") {
        val m = Matrix(3,5)
        m(2) zip Array(10, 11, 12, 13, 14) foreach {
          case (x1,x2) => assert(x1 == x2)
        }
      }
    }

    describe ("apply(i,j)") {
      it ("returns cell (i,j) (zero-based indexing)") {
        check(3, 5)
      }
    }
  }

  private def check(m: Int, n: Int): Unit = {
    val matrix = Matrix(m, n)
    for {
      i <- 0 to (m-1)
      j <- 0 to (n-1)
    } assert(matrix(i,j) == (5*i)+j, s"($i, $j): ${matrix(i,j)} ==? ${i+j}")
  }
}
