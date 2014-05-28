package spark
import spark.util.Timestamp
import org.scalatest.FunSpec

class Joins7Spec extends FunSpec {

  describe ("Joins7") {
    it ("computes the join of the bible book abbreviations with their full names") {
      Timestamp.isTest = true
      val out     = "output/kjv-joins"
      val out2    = out+"-"
      val golden  = "golden/kjv-joins/part-00000"
      
      Joins7.main(Array(
        "--quiet", "--inpath", "data/kjvdat.txt", 
        "--abbreviations", "data/abbrevs-to-names.tsv", "--outpath", out))

      TestUtil.verifyAndClean(s"$out2/part-00000", golden, out2)
    }
  }
}