import org.scalatest.FunSpec
import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}

// Run in local mode and local data.
class Matrix4Spec extends FunSpec {

  describe ("Matrix4") {
    it ("computes the sums of the rows in parallel.") {
      // Redirect from stdout:
      val byteStream = new ByteArrayOutputStream(512)
      Matrix4.out = new PrintStream(byteStream, true)
      val golden = """5x10 Matrix:
        |Row # 0: Sum =   45, Avg =   4
        |Row # 1: Sum =  145, Avg =  14
        |Row # 2: Sum =  245, Avg =  24
        |Row # 3: Sum =  345, Avg =  34
        |Row # 4: Sum =  445, Avg =  44
        |""".stripMargin

      Matrix4.main(Array("--master", "local", "5", "10"))
      assert(byteStream.toString === golden)
    }
  }
}