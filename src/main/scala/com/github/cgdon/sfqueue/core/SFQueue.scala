package com.github.cgdon.sfqueue.core

import java.io.File

/**
  * Created by 成国栋 on 2017-11-11 00:26:00.
  */
class SFQueue(dir: File, maxDataFileLength: Int = 1 << 30) {

  def this(dirPath: String, maxDataFileLength: Int = 1 << 30) = {
    this(new File(dirPath), maxDataFileLength)
  }




}
