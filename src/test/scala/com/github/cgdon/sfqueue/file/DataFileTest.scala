package com.github.cgdon.sfqueue.file

import org.specs2.mutable.Specification

class DataFileTest extends Specification {

  "DataFileTest" should {
    "getIndexByFileName" in {
      val index = DataFile.getIndexByFileName("sfq_100.dat")
      println(s"index: $index")
      ok
    }

  }
}
