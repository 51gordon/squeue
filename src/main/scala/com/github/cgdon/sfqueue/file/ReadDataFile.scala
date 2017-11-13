package com.github.cgdon.sfqueue.file

import java.io.File

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class ReadDataFile(dir: File, index: Int, initMaxLength: Int) extends DataFile(dir, index, initMaxLength) {

  var readPos: Int = DATA_HEADER_LENGTH

  /**
    * 是否还有数据
    *
    * @return
    */
  def hasNext(): Boolean = readPos < FILE_LIMIT

  /**
    * 读取一条数据
    *
    * @param commit 是否自动提交
    * @return
    */
  def read(commit: Boolean): Option[Array[Byte]] = {
    mbBuffer.getInt() match {
      case len if len > 0 =>
        val buf = new Array[Byte](len)
        mbBuffer.get(buf)
        Some(buf)
      case _ =>
        None
    }
  }

  /**
    * 下一条数据所占空间
    *
    * @return
    */
  def nextLen(): Int = {
    4 + mbBuffer.getInt()
  }
}

