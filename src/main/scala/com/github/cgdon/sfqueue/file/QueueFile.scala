package com.github.cgdon.sfqueue.file

import java.io.{ File, RandomAccessFile }
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

import com.github.cgdon.sfqueue.ex.FileFormatException

/**
  * queue文件接口
  * Created by 成国栋 on 2017-11-11 00:44:00.
  */
trait QueueFile extends AutoCloseable {

  val version = 1
  var raFile: RandomAccessFile = _
  var fc: FileChannel = _
  var mbBuffer: MappedByteBuffer = _

  /**
    * 返回该文件的magic
    *
    * @return
    */
  def magic(): String

  def init(f: File, initMaxLength: Int): Unit = {
    if (!f.exists()) {
      f.createNewFile()
      initMemoryMapFile(f, initMaxLength)
      // 初始化文件
      initFile()
    } else {
      initMemoryMapFile(f, initMaxLength)
    }

    // 加载文件
    loadFile()
  }

  /**
    * 初始化内存文件映射相关对象
    */
  def initMemoryMapFile(f: File, initMaxLength: Int): Unit = {
    raFile = new RandomAccessFile(f, "rwd")
    fc = raFile.getChannel
    mbBuffer = fc.map(MapMode.READ_WRITE, 0, initMaxLength)
  }

  /**
    * 读取magic
    *
    * @param buffer
    * @return
    */
  def readMagic(buffer: MappedByteBuffer): String = {
    // read magic
    val magicBuf = new Array[Byte](8)
    buffer.get(magicBuf)
    val _magic = new String(magicBuf)
    if (_magic != magic()) {
      throw new FileFormatException("Index file format error, magic incorrect!")
    }
    _magic
  }

  /**
    * 从空文件初始化为相应格式的文件
    */
  def initFile(): Unit

  /**
    * 加载文件到内存
    */
  def loadFile(): Unit

  /**
    * 关闭文件资源
    */
  override def close(): Unit = {
    mbBuffer.force().clear()
    fc.close()
    raFile.close()
  }

}
