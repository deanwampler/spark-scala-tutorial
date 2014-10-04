import org.scalatest.FunSpec

// Run in local mode and local data.
class Crawl5aSpec extends FunSpec {

  describe ("Crawl5a") {
    it ("simulates a web crawler") {
      val out    = "output/crawl"
      val out2   = s"$out/part-00000"
      val golden = "golden/crawl/part-00000"
      TestUtil.rmrf(out)  // Delete previous runs, if necessary.

      // The defaults for --input and --output are fine:
      Crawl5a.main(Array("--master", "local", "--quiet"))

      TestUtil.verifyAndClean(out2, golden, out)
    }
  }
}