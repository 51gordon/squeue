package com.github.cgdon.sfqueue.file

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import com.github.cgdon.sfqueue.ex.FileFormatException

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class IndexFile(parent: File) extends QueueFile {
  private val INDEX_FILE_NAME = "sfq.idx"
  private val INDEX_LIMIT_LENGTH = 36

  // file index and offset
  var readIdx: Int = 1
  var readPos: Int = 0
  var writeIdx: Int = 1
  var writePos: Int = 0
  val queuSize = new AtomicLong()

  private val idxFile = new File(parent, INDEX_FILE_NAME)
  init(idxFile, INDEX_LIMIT_LENGTH)

  override def magic(): String = "sfqueidx"

  /**
    * 初始化文件
    */
  override def initFile(): Unit = {
    raFile.writeUTF(magic()) // write magic(start: 0)
    raFile.writeInt(version) // write version(start:8)
    raFile.writeInt(1) // write read index(start:12)
    raFile.writeInt(0) // write read (start:16)
    raFile.writeInt(1) // write write index(start:20)
    raFile.writeInt(0) // write write pos(start:24)
    // write size(long)
    raFile.writeLong(0L) // // write write pos(start:28)
  }

  /**
    * 加载文件
    */
  override def loadFile(): Unit = {
    if (raFile.length() < INDEX_LIMIT_LENGTH) {
      throw new FileFormatException("Index file format error, length incorrect!")
    }
    val bytes = new Array[Byte](INDEX_LIMIT_LENGTH)
    val buffer = ByteBuffer.wrap(bytes)

    readMagic(mbBuffer)

    readIdx = buffer.getInt
    readPos = buffer.getInt()
    writeIdx = buffer.getInt()
    writePos = buffer.getInt()
    queuSize.set(buffer.getLong())
  }

  def putReadIdx(idx: Int): Unit = {
    mbBuffer.position(12)
    mbBuffer.putInt(idx)
    this.readIdx = idx
  }

  def forwardReadPos(delta: Int): Unit = {
    mbBuffer.position(16)
    val _pos = mbBuffer.getInt() + delta
    mbBuffer.putInt(_pos)
    this.readPos = _pos
  }

  def putWriteIdx(idx: Int): Unit = {
    mbBuffer.position(20)
    mbBuffer.putInt(idx)
    this.writeIdx = idx
  }

  def forwardWritePos(delta: Int): Unit = {
    mbBuffer.position(24)
    val _pos = mbBuffer.getInt() + delta
    mbBuffer.putInt(_pos)
    this.writePos = _pos
  }

  def incrementSize(): Unit = {
    val newSize = queuSize.incrementAndGet()
    mbBuffer.position(28)
    mbBuffer.putLong(newSize)
  }

  def decrementSize(): Unit = {
    val newSize = queuSize.decrementAndGet()
    mbBuffer.position(28)
    mbBuffer.putLong(newSize)
  }

  def size(): Long = {
    queuSize.get()
  }

  /**
    * 清空索引文件内容
    */
  def clear(): Unit = {
    mbBuffer.clear()
    mbBuffer.force()
    initFile()
  }
}
