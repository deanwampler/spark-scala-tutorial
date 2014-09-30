package com.typesafe.sparkworkshop
import com.typesafe.sparkworkshop.util.Timestamp
import org.scalatest.FunSpec
import java.io.{File, FileOutputStream, PrintStream}

class NGrams6Spec extends FunSpec {

  describe ("NGrams6") {
    it ("computes ngrams from text") {
      Timestamp.isTest = true
      val out    = "output/ngrams.txt"
      val golden = "golden/ngrams/100.txt"
      TestUtil.rmrf(out)  // Delete previous runs, if necessary.

      val fileStream = new FileOutputStream(new File(out))
      NGrams6.out = new PrintStream(fileStream, true)

      NGrams6.main(Array(
        "--quiet", "--inpath", "data/kjvdat.txt",
        "--count", "100", "--ngrams", "% love % %"))

      TestUtil.verifyAndClean(out, golden, out)
    }
  }
}