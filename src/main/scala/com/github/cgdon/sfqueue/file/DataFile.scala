package com.github.cgdon.sfqueue.file

import java.io.File

import com.github.cgdon.sfqueue.ex.SFQueueException

import scala.util.matching.Regex

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class DataFile(val dir: File, val index: Int, initMaxLength: Int) extends QueueFile {

  var endPos: Int = DATA_HEADER_LENGTH

  val datFile = new File(dir, getFileName(index))
  init(datFile, initMaxLength)

  override def magic(): String = "sfquedat"

  override def initFile(): Unit = {
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

  def readEndPos(): Int = {
    mbBuffer.position(12)
    mbBuffer.getInt()
  }

  /**
    * 读取writePos，写入到endPos
    */
  def writeEndPos(endPos: Int): Unit = {
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
        throw SFQueueException(s"Invalid sfqueue data file name: $name")
    }
  }

  def isDataFile(name: String) = name.matches(matchPattern)
}