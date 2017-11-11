package com.github.cgdon.sfqueue.file

import java.io.File

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class WriteDataFile(dir: File, index: Int, initMaxLength: Int, var writePos: Int) extends DataFile(dir, index, initMaxLength) {

  def available(buf: Array[Byte]): Int = {
    FILE_LIMIT_LENGTH - (writePos + 4 + buf.length)
  }

  def write(buf: Array[Byte]): Int = {
    val delta = 4 + buf.length
    mbBuffer.position(writePos)
    mbBuffer.putInt(buf.length)
    mbBuffer.put(buf)
    writePos += delta
    delta
  }

}

