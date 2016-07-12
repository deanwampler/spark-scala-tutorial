import org.scalatest.FunSpec
import util.FileUtil

// Run in local mode and local data.
class SparkSQL8Spec extends FunSpec {

  describe ("SparkSQL8") {
    it ("runs SQL queries against an RDD") {
      val out     = "output/kjv-spark-sql"
      val outvpb  = s"$out-verses-per-book"
      val outgv   = s"$out-god-verses"
      val goldenvpb = "golden/kjv-spark-sql-verses-per-book/part-00000"
      val goldengv0 = "golden/kjv-spark-sql-god-verses/part-00000"
      val goldengv1 = "golden/kjv-spark-sql-god-verses/part-00001"
      FileUtil.rmrf(outvpb)  // Delete previous runs, if necessary.
      FileUtil.rmrf(outgv)   // Delete previous runs, if necessary.

      SparkSQL8.main(Array(
        "--master", "local[2]", "--quiet", "--inpath", "data/kjvdat.txt",
        "--outpath", out))

      try {
        TestUtil.verifyAndClean(s"$outvpb/part-00000", goldenvpb, outvpb)
        TestUtil.verify(s"$outgv/part-00000", goldengv0)
        TestUtil.verify(s"$outgv/part-00001", goldengv1)
      } finally {
        FileUtil.rmrf(outgv)
      }
    }
  }
}