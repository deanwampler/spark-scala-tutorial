package spark
import spark.util.Timestamp
import org.scalatest.FunSpec

class InvertedIndex5bSpec extends FunSpec {

  describe ("InvertedIndex5b") {
    it ("computes the famous 'inverted index' from web crawl data") {
      Timestamp.isTest = true
      val out    = "output/inverted-index"
      val out2   = s"$out-/part-00000"
      val golden = "golden/inverted-index/part-00000"

      // We have to run the Crawl first to ensure the data exists!
      Crawl5a.main(Array("--quiet"))

      InvertedIndex5b.main(Array(
        "--quiet", "--inpath", "output/crawl", "--outpath", out))

      TestUtil.verifyAndClean(out2, golden, out+"-")
      TestUtil.rmrf("output/crawl")
    }
  }
}