package com.github.cgdon.sfqueue.core

import java.io.File

import com.github.cgdon.sfqueue.file._

/**
  * Created by 成国栋 on 2017-11-11 00:26:00.
  */
class SFQueue(dir: File, maxDataFileLength: Int = 1 << 21) {

  var readDataFile: ReadDataFile = _
  var writeDataFile: WriteDataFile = _

  dir.mkdirs()
  // 初始化索引文件和待处理的数据文件
  val idxFile = new IndexFile(dir)
  initDataFiles()

  def this(dirPath: String) = {
    this(new File(dirPath))
  }

  def offer(buf: Array[Byte]): Boolean = {
    if (writeDataFile.available(buf) <= 0) {
      rotateWriteFile()
    }
    val delta = writeDataFile.write(buf)
    idxFile.forwardReadPos(delta)
    idxFile.incrementSize()
    true
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

  def size(): Long = idxFile.size()


  def clear(): Unit = {
    // 清空
//    idxFile.clear()
//    initDataFiles()
    throw new UnsupportedOperationException()
  }

  def close(): Unit = {
    writeDataFile.close()
    readDataFile.close()
    idxFile.close()
  }

  private def initDataFiles(): Unit = {
    readDataFile = new ReadDataFile(dir, idxFile.readIdx, maxDataFileLength)
    readDataFile.readPos = idxFile.readPos
    writeDataFile = new WriteDataFile(dir, idxFile.writeIdx, maxDataFileLength)
    writeDataFile.writePos = idxFile.writePos
  }

  private def rotateReadFile(): Unit = {
    val newIndex = readDataFile.index + 1
    readDataFile.close()
    readDataFile = new ReadDataFile(dir, newIndex, maxDataFileLength)
  }

  private def rotateWriteFile(): Unit = {
    val newIndex = writeDataFile.index + 1
    writeDataFile.close()
    writeDataFile = new WriteDataFile(dir, newIndex, maxDataFileLength)
  }

}
