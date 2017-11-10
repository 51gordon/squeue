package com.github.cgdon.sfqueue.file

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.atomic.AtomicLong

import com.github.cgdon.sfqueue.ex.FileFormatException

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class IndexFile(idxFile: File) extends QueueFile {
  private val INDEX_FILE_NAME = "sfq.idx"
  private val INDEX_LIMIT_LENGTH = 36

  // file index and offset
  private var version = 1
  private var readIdx: Int = 1
  private var readPos: Int = 0
  private var writeIdx: Int = 1
  private var writePos: Int = 0
  private val size = new AtomicLong()

  private val dbRandFile = new RandomAccessFile(idxFile, "rwd")
  private var fc: FileChannel = _
  private var mappedByteBuffer = fc.map(MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH)

  init(idxFile)

  override def magic(): String = "sfqueidx"

  /**
    * 初始化文件
    */
  override def initFile(): Unit = {
    dbRandFile.setLength(1111)
    dbRandFile.seek(0)
    dbRandFile.writeUTF(magic()) // write magic(0)
    dbRandFile.writeInt(version) // write version(8)
    dbRandFile.writeInt(readIdx) // write read index(12)
    dbRandFile.writeInt(readPos) // write read (16)
    dbRandFile.writeInt(writeIdx) // write write index(20)
    dbRandFile.writeInt(writePos) // write write pos(24)
    // write size(long)
    dbRandFile.writeLong(0L) // // write write pos(32)
  }

  /**
    * 加载文件
    */
  override def loadFile(): Unit = {
    if (dbRandFile.length() < INDEX_LIMIT_LENGTH) {
      throw new FileFormatException("Index file format error, length incorrect!")
    }
    val bytes = new Array[Byte](INDEX_LIMIT_LENGTH)
    val buffer = ByteBuffer.wrap(bytes)

    // read magic
    val magicBuf = new Array[Byte](8)
    buffer.get(magicBuf)
    val _magic = new String(magicBuf)
    if (_magic != magic()) {
      throw new FileFormatException("Index file format error, magic incorrect!")
    }

    version = buffer.getInt()
    readIdx = buffer.getInt
    readPos = buffer.getInt()
    writeIdx = buffer.getInt()
    writePos = buffer.getInt()
    size.set(buffer.getLong())

    fc = dbRandFile.getChannel
    mappedByteBuffer = fc.map(MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH)
  }

  def putWritePos(pos: Int) = {
    mappedByteBuffer.position(24)
    mappedByteBuffer.putInt(pos)
    this.writePos = pos
  }
}
