package com.typesafe.sparkworkshop
import com.typesafe.sparkworkshop.util.Timestamp
import org.scalatest.FunSpec

class WordCount2Spec extends FunSpec {

  describe ("WordCount2") {
    it ("computes the word count of the input corpus") {
      Timestamp.isTest = true
      val out    = "output/kjv-wc2-"
      val out2   = s"$out/part-00000"
      val golden = "golden/kjv-wc2/part-00000"
      TestUtil.rmrf(out)  // Delete previous runs, if necessary.

      WordCount2.main(Array.empty[String])

      TestUtil.verifyAndClean(out2, golden, out)
    }
  }
}