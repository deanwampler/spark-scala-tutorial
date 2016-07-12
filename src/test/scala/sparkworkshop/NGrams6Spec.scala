import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}
import util.FileUtil

// Run in local mode and local data.
class NGrams6Spec extends FunSpec {

  describe ("NGrams6") {
    it ("computes ngrams from text") {
      val out    = "output/ngrams"
      val golden = "golden/ngrams/part-00000"
      FileUtil.rmrf(out)  // Delete previous runs, if necessary.

      NGrams6.main(Array(
        "--master", "local", "--quiet", "--inpath", "data/kjvdat.txt",
        "--count", "100", "--ngrams", "% love % %"))

      TestUtil.verifyAndClean(s"$out/part-00000", golden, out)
    }
  }
}