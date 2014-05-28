package spark
import spark.util.Timestamp
import org.scalatest.FunSpec

class WordCount3Spec extends FunSpec {

  describe ("WordCount3") {
    it ("computes the word count of the input corpus with options") {
      Timestamp.isTest = true
      val out    = "output/kjv-wc3"
      val out2   = out+"-"
      val golden = "golden/kjv-wc3/data.txt"

      WordCount3.main(Array(
        "--quiet", "--inpath", "data/kjvdat.txt", "--outpath", out))

      TestUtil.verifyAndClean(out2, golden, out2)
    }
  }
}