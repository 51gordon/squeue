package com.github.cgdon.squeue.file

import java.io.File

import com.github.cgdon.squeue.ex.QueueException

import scala.util.matching.Regex

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class DataFile(val dir: File, val index: Int, dataFileSizeMb: Int) extends QueueFile {

  var endPos: Int = DATA_HEADER_LENGTH

  val datFile = new File(dir, getFileName(index))
  init(datFile, dataFileSizeMb * (1 << 20) + DATA_HEADER_LENGTH + 4)

  override def magic(): String = "sque_dat"

  override def createFile(): Unit = {
    mbBuffer.put(magic().getBytes(MAGIC_CHARSET)) // put magic(start: 0)
    mbBuffer.putInt(version) // put version(start:8)
    mbBuffer.putInt(endPos) // put recordNum(start:12)
  }

  override def loadFile(): Unit = {
    mbBuffer.position(0)
    readMagic()
    mbBuffer.getInt // version
    endPos = mbBuffer.getInt
  }

  def readEndPosFromFile(): Int = {
    mbBuffer.position(12)
    mbBuffer.getInt()
  }

  /**
    * 读取writePos，写入到endPos
    */
  def writeEndPos2File(endPos: Int): Unit = {
    mbBuffer.position(12)
    mbBuffer.putInt(endPos)
  }

  def getFileName(index: Int) = s"sfq_$index.dat"

  def getIndexByFileName: Int = DataFile.getIndexByFileName(datFile.getName)
}

object DataFile {
  val matchPattern: String = "sfq_\\d+\\.dat"
  val capturePattern: Regex = "sfq_(\\d+)\\.dat".r

  def getIndexByFileName(name: String): Int = {
    capturePattern.findFirstMatchIn(name) match {
      case Some(a) => a.group(1).toInt
      case None =>
        throw QueueException(s"Invalid squeue data file name: $name")
    }
  }

  def isDataFile(name: String) = name.matches(matchPattern)
}