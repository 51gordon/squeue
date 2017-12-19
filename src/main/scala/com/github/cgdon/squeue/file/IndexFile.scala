package com.github.cgdon.squeue.file

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import com.github.cgdon.squeue.ex.QueueException

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
  var totalInRecordNum: Long = 0
  var totalInRecordSize: Long = 0
  var totalOutRecordNum: Long = 0
  var totalOutRecordSize: Long = 0

  private val idxFile = new File(parent, IndexFile.INDEX_FILE_NAME)
  init(idxFile, IndexFile.INDEX_LIMIT_LENGTH)

  override def magic(): String = "sque_idx"

  /**
    * 初始化文件
    */
  override def initEmptyFile(): Unit = {
    mbBuffer.position(0)
    mbBuffer.put(magic().getBytes(MAGIC_CHARSET)) // magic(start:0)
    mbBuffer.putInt(version) // version(start:8)
    mbBuffer.putInt(1) // put read index(start:12)
    mbBuffer.putInt(DATA_HEADER_LENGTH) // put read pos(start:16)
    mbBuffer.putInt(1) // put write index(start:20)
    mbBuffer.putInt(DATA_HEADER_LENGTH) // put write pos(start:24)
    mbBuffer.putLong(0L) // put size pos(start:28)
    mbBuffer.putLong(0L) // put total in record num(start:36)
    mbBuffer.putLong(0L) // put total out record num(start:44)
    mbBuffer.putLong(0L) // put total in record size(start:52)
    mbBuffer.putLong(0L) // put total out record size(start:60)
  }

  /**
    * 加载文件
    */
  override def loadFile(): Unit = {
    if (raFile.length() < IndexFile.INDEX_LIMIT_LENGTH) {
      throw QueueException("Index file format error, length incorrect!")
    }
    mbBuffer.position(0)
    readMagic()
    mbBuffer.getInt() // version
    readIdx = mbBuffer.getInt
    readPos = mbBuffer.getInt()
    writeIdx = mbBuffer.getInt()
    writePos = mbBuffer.getInt()
    queueSize.set(mbBuffer.getLong())
    totalInRecordNum = mbBuffer.getLong
    totalOutRecordNum = mbBuffer.getLong
    totalInRecordSize = mbBuffer.getLong
    totalOutRecordSize = mbBuffer.getLong
  }

  //  /**
  //    * 更新读文件的index
  //    *
  //    * @param idx
  //    */
  //  def updateReadIdx(idx: Int): Unit = {
  //    mbBuffer.position(12)
  //    mbBuffer.putInt(idx)
  //    this.readIdx = idx
  //  }

  /**
    * 滚动读文件的位置
    *
    * @param posDelta 读位置的增量
    */
  private def forwardReadPos(posDelta: Int): Unit = {
    val newPos = readPos + posDelta
    mbBuffer.position(16)
    mbBuffer.putInt(newPos)
    this.readPos = newPos
  }

  def resetReadPos(): Unit = {
    readPos = DATA_HEADER_LENGTH
  }

  def resetWritePos(): Unit = {
    writePos = DATA_HEADER_LENGTH
  }

  //  /**
  //    * 更新写文件的index
  //    *
  //    * @param idx
  //    */
  //  def updateWriteIdx(idx: Int): Unit = {
  //    mbBuffer.position(20)
  //    mbBuffer.putInt(idx)
  //    this.writeIdx = idx
  //  }

  /**
    *
    * 滚动写文件的位置
    *
    * @param posDelta 写位置的增量
    */
  private def forwardWritePos(posDelta: Int): Unit = {
    val newPos = writePos + posDelta
    mbBuffer.position(24)
    mbBuffer.putInt(newPos)
    this.writePos = newPos
  }

  private def updateQueueSize(newSize: Long): Unit = {
    mbBuffer.position(28)
    mbBuffer.putLong(newSize)
  }

  def incrementSize(msgLen: Int): Unit = {
    incrementSize(1, msgLen)
  }

  def incrementSize(msgNum: Int, totalMsgLen: Int): Unit = {
    // 队列数据条数 + msgNum
    updateQueueSize(queueSize.addAndGet(msgNum))

    // 写位置 + (4 * msgNum + totalMsgLen)
    forwardWritePos(4 * msgNum + totalMsgLen)

    // 总的in数据条数 + msgNum
    totalInRecordNum += msgNum

    // 总的in数据字节数+totalMsgLen
    totalInRecordSize += totalMsgLen
  }

  def decrementSize(msgLen: Int): Unit = {
    decrementSize(1, msgLen)
  }

  def decrementSize(msgNum: Int, totalMsgLen: Int): Unit = {
    // 队列数据条数 - msgNum
    updateQueueSize(queueSize.addAndGet(-msgNum))

    // 读位置 + (4 * msgNum + totalMsgLen)
    forwardReadPos(4 * msgNum + totalMsgLen)

    // 总的out数据条数+msgNum
    totalOutRecordNum += msgNum

    // 总的out数据字节数+totalMsgLen
    totalOutRecordSize += totalMsgLen
  }

  /**
    * 队列数据条数
    *
    * @return
    */
  def size(): Long = {
    queueSize.get()
  }

  /**
    * 清空索引文件内容
    */
  def clear(): Unit = {
    mbBuffer.clear()
    mbBuffer.force()
    initEmptyFile()
    loadFile()
  }

  def clearStatData(): Unit = {
    totalInRecordNum = 0
    totalInRecordSize = 0
    totalOutRecordNum = 0
    totalOutRecordSize = 0
    mbBuffer.position(36)
    mbBuffer.putLong(0)
    mbBuffer.putLong(0)
    mbBuffer.putLong(0)
    mbBuffer.putLong(0)
  }
}

object IndexFile {

  // 索引文件名
  val INDEX_FILE_NAME = "sfq.idx"

  //  8, magic
  //  4, version
  //  4, read index
  //  4, read pos
  //  4, write index
  //  4, write pos
  //  8, queue size
  //  8, total in record num
  //  8, total out record num
  //  8, total in record size
  //  8, total out record size
  val INDEX_LIMIT_LENGTH = 68
}
