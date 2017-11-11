package com.github.cgdon.sfqueue.file

import java.io.File

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class DataFile(dir: File, index: Int, initMaxLength: Int) extends QueueFile {

  var recordNum: Int = 0

  private val datFile = new File(dir, getFileName(index))
  val FILE_LIMIT_LENGTH: Int = math.max(raFile.length().toInt, initMaxLength)
  init(datFile, FILE_LIMIT_LENGTH)

  override def magic(): String = "sfquedat"

  override def initFile(): Unit = {
    raFile.writeUTF(magic()) // write magic(start: 0)
    raFile.writeInt(version) // write version(start:8)
    raFile.writeInt(recordNum) // write recordNum(start:12)
    raFile.writeInt(-1) // write writePos(start:16)
  }

  override def loadFile(): Unit = {
    readMagic(mbBuffer)
    mbBuffer.getInt // version
    recordNum = mbBuffer.getInt
  }

  def getFileName(index: Int) = s"sfq_$index.dat"

}

object Signal extends Enumeration {
  type Signal = Value
  val WRITE_FULL, WRITE_SUC = Value
}