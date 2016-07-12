import org.scalatest.FunSpec
import util.FileUtil

// Run in local mode and local data.
class Joins7Spec extends FunSpec {

  describe ("Joins7") {
    it ("computes the join of the bible book abbreviations with their full names") {
      val out     = "output/kjv-joins"
      val golden  = "golden/kjv-joins/part-00000"
      FileUtil.rmrf(out)  // Delete previous runs, if necessary.

      Joins7.main(Array(
        "--master", "local", "--quiet", "--inpath", "data/kjvdat.txt",
        "--abbreviations", "data/abbrevs-to-names.tsv", "--outpath", out))

      TestUtil.verifyAndClean(s"$out/part-00000", golden, out)
    }
  }
}