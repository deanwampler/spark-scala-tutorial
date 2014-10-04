import org.scalatest.FunSpec

// Run in local mode and local data.
class SparkSQL9Spec extends FunSpec {

  describe ("SparkSQL9") {
    it ("computes the join of the bible book abbreviations with their full names") {
      val out     = "output/spark-sql"
      val outvpb  = s"$out-verses-per-book"
      val outgv   = s"$out-god-verses"
      val goldenvpb = "golden/spark-sql-verses-per-book/part-00000"
      val goldengv0 = "golden/spark-sql-god-verses/part-00000"
      val goldengv1 = "golden/spark-sql-god-verses/part-00001"
      TestUtil.rmrf(outvpb)  // Delete previous runs, if necessary.
      TestUtil.rmrf(outgv)   // Delete previous runs, if necessary.

      SparkSQL9.main(Array(
        "--master", "local[2]", "--quiet", "--inpath", "data/kjvdat.txt",
        "--outpath", out))

      try {
        TestUtil.verifyAndClean(s"$outvpb/part-00000", goldenvpb, outvpb)
        TestUtil.verify(s"$outgv/part-00000", goldengv0)
        TestUtil.verify(s"$outgv/part-00001", goldengv1)
      } finally {
        TestUtil.rmrf(outgv)
      }
    }
  }
}