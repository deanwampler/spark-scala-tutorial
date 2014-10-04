import org.scalatest.FunSpec

// Run in local mode and local data.
class InvertedIndex5bSpec extends FunSpec {

  describe ("InvertedIndex5b") {
    it ("computes the famous 'inverted index' from web crawl data") {
      val out    = "output/inverted-index"
      val out2   = s"$out/part-00000"
      val golden = "golden/inverted-index/part-00000"
      TestUtil.rmrf(out)  // Delete previous runs, if necessary.

      // We have to run the Crawl first to ensure the data exists!
      TestUtil.rmrf("output/crawl")  // Delete previous runs, if necessary.
      Crawl5a.main(Array("--master", "local", "--quiet"))

      InvertedIndex5b.main(Array(
        "--master", "local", "--quiet", "--inpath", "output/crawl", "--outpath", out))

      TestUtil.verifyAndClean(out2, golden, out+"-")
      TestUtil.rmrf("output/crawl")
    }
  }
}