package spark
import spark.util.Timestamp
import org.scalatest.FunSpec

class Crawl5aSpec extends FunSpec {

  describe ("Crawl5a") {
    it ("simulates a web crawler") {
      Timestamp.isTest = true
      val out    = "output/crawl"
      val out2   = s"$out/part-00000"
      val golden = "golden/crawl/part-00000"
      TestUtil.rmrf(out)  // Delete previous runs, if necessary.
      
      // The defaults for --input and --output are fine:
      Crawl5a.main(Array("--quiet"))
      
      TestUtil.verifyAndClean(out2, golden, out)
    }
  }
}