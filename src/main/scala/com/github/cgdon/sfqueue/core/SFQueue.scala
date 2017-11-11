package com.github.cgdon.sfqueue.core

import java.io.File

import com.github.cgdon.sfqueue.file.{ IndexFile, ReadDataFile, WriteDataFile }

/**
  * Created by 成国栋 on 2017-11-11 00:26:00.
  */
class SFQueue(dir: File, maxDataFileLength: Int = 1 << 30) {

  val idxFile = new IndexFile(dir)

  val readDataFile: ReadDataFile =
    new ReadDataFile(dir, idxFile.readIdx, maxDataFileLength, idxFile.readPos)
  val writeDataFile: WriteDataFile =
    new WriteDataFile(dir, idxFile.writeIdx, maxDataFileLength, idxFile.writePos)

  def this(dirPath: String, maxDataFileLength: Int = 1 << 30) = {
    this(new File(dirPath), maxDataFileLength)
  }

  def offer(buf: Array[Byte]): Unit = {
    if (writeDataFile.available(buf) <= 0) {
      rotateWriteFile()
    }
    val delta = writeDataFile.write(buf)
    idxFile.forwardReadPos(delta)
    idxFile.incrementSize()
  }

  def peek(): Option[Array[Byte]] = {
    if (readDataFile.hasNext()) {
      readDataFile.read(false)
    } else if (idxFile.size() > 0) {
      rotateReadFile()
      readDataFile.read(false)
    } else {
      None
    }
  }

  def poll(): Option[Array[Byte]] = {
    if (readDataFile.hasNext()) {
      readDataFile.read(true)
    } else if (idxFile.size() > 0) {
      rotateReadFile()
      readDataFile.read(true)
    } else {
      None
    }
  }

  def remove(): Unit = {
    if (readDataFile.hasNext()) {
      idxFile.forwardReadPos(readDataFile.nextLen())
    } else if (idxFile.size() > 0) {
      rotateReadFile()
      idxFile.forwardReadPos(readDataFile.nextLen())
    }
  }

  def clear(): Unit = {
    // todo
  }

  def close(): Unit = {
    writeDataFile.close()
    readDataFile.close()
    idxFile.close()
  }

  def rotateWriteFile(): Unit = {

  }

  def rotateReadFile(): Unit = {

  }

}
