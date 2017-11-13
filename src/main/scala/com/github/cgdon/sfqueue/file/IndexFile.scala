package com.github.cgdon.sfqueue.file

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.github.cgdon.sfqueue.ex.SFQueueException

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class IndexFile(parent: File) extends QueueFile {

  // file index and offset
  var readIdx: Int = -1
  var readPos: Int = -1
  var writeIdx: Int = -1
  var writePos: Int = -1
  val queueSize = new AtomicLong(0)

  private val idxFile = new File(parent, IndexFile.INDEX_FILE_NAME)
  init(idxFile, IndexFile.INDEX_LIMIT_LENGTH)

  override def magic(): String = "sfqueidx"

  /**
    * 初始化文件
    */
  override def initFile(): Unit = {
    mbBuffer.put(magic().getBytes(MAGIC_CHARSET))
    mbBuffer.putInt(version)
    mbBuffer.putInt(1) // put read index(start:12)
    mbBuffer.putInt(DATA_HEADER_LENGTH) // put read pos(start:16)
    mbBuffer.putInt(1) // put write index(start:20)
    mbBuffer.putInt(DATA_HEADER_LENGTH) // put write pos(start:24)
    mbBuffer.putLong(0L) // put size pos(start:28)
  }

  /**
    * 加载文件
    */
  override def loadFile(): Unit = {
    if (raFile.length() < IndexFile.INDEX_LIMIT_LENGTH) {
      throw SFQueueException("Index file format error, length incorrect!")
    }
    mbBuffer.position(0)
    readMagic()
    mbBuffer.getInt() // version
    readIdx = mbBuffer.getInt
    readPos = mbBuffer.getInt()
    writeIdx = mbBuffer.getInt()
    writePos = mbBuffer.getInt()
    queueSize.set(mbBuffer.getLong())
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
    val newSize = queueSize.incrementAndGet()
    mbBuffer.position(28)
    mbBuffer.putLong(newSize)
  }

  def decrementSize(): Unit = {
    val newSize = queueSize.decrementAndGet()
    mbBuffer.position(28)
    mbBuffer.putLong(newSize)
  }

  def size(): Long = {
    queueSize.get()
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

object IndexFile {
  val INDEX_FILE_NAME = "sfq.idx"
  val INDEX_LIMIT_LENGTH = 36
}
