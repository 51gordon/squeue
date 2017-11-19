package com.github.cgdon.sfqueue.file

import java.io.File

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class ReadDataFile(dir: File, index: Int, dataFileSizeMb: Int) extends DataFile(dir, index, dataFileSizeMb) {

  var pos: Int = DATA_HEADER_LENGTH

  /**
    * 读取一条数据
    *
    * @return
    */
  def readNext(): Array[Byte] = {
    mbBuffer.position(pos)
    val len = mbBuffer.getInt
    val buf = new Array[Byte](len)
    mbBuffer.get(buf)
    buf
  }

  /**
    * 删除一条数据
    *
    * @return 返回被移除数据的字节数
    */
  def remove(): Int = {
    mbBuffer.position(pos)
    val len = mbBuffer.getInt
    mbBuffer.position(pos + 4 + len)
    len
  }
}

