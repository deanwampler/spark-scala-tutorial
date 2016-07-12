import org.scalatest.FunSpec
import util.FileUtil

// Run in local mode and local data.
class WordCount3Spec extends FunSpec {

  describe ("WordCount3") {
    it ("computes the word count of the input corpus with options") {
      val out    = "output/kjv-wc3"
      val out2   = s"$out/part-00000"
      val golden = "golden/kjv-wc3/part-00000"
      FileUtil.rmrf(out)  // Delete previous runs, if necessary.

      WordCount3.main(Array(
        "--master", "local", "--quiet", "--inpath", "data/kjvdat.txt", "--outpath", out))

      TestUtil.verifyAndClean(out2, golden, out2)
    }
  }
}