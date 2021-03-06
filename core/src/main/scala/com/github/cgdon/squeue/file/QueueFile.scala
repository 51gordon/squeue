package com.github.cgdon.squeue.file

import java.io.{ File, RandomAccessFile }
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode

import com.github.cgdon.squeue.ex.QueueException

/**
  * queue文件接口
  * Created by 成国栋 on 2017-11-11 00:44:00.
  */
trait QueueFile extends AutoCloseable {

  val version = 1

  var FILE_LIMIT: Int = _
  val DATA_HEADER_LENGTH = 16
  var raFile: RandomAccessFile = _
  var fc: FileChannel = _
  var mbBuffer: MappedByteBuffer = _

  val MAGIC_CHARSET = "iso-8859-1"

  /**
    * 返回该文件的magic
    *
    * @return
    */
  def magic(): String

  def init(f: File, initMaxLength: Int): Unit = {
    if (!f.exists()) {
      f.createNewFile()
      initMbBuffer(f, initMaxLength)
      // 初始化文件
      initEmptyFile()
    } else {
      initMbBuffer(f, initMaxLength)
    }

    // 加载文件
    loadFile()
  }

  /**
    *
    * 初始化内存文件映射相关对象，此操作略微耗时，相比close的性能损耗，还是少很多
    *
    * @param f             文件
    * @param initMaxLength 初始时文件最大长度
    * @return
    */
  def initMbBuffer(f: File, initMaxLength: Int): Unit = {
    raFile = new RandomAccessFile(f, "rwd")
    fc = raFile.getChannel
    FILE_LIMIT = math.max(f.length().toInt, initMaxLength)
    mbBuffer = fc.map(MapMode.READ_WRITE, 0, FILE_LIMIT)
  }

  /**
    * 读取magic
    *
    * @return
    */
  def readMagic(): String = {
    // read magic
    val magicBuf = new Array[Byte](8)
    mbBuffer.get(magicBuf)
    val _magic = new String(magicBuf)
    if (_magic != magic()) {
      throw QueueException("Index file format error, magic incorrect!")
    }
    _magic
  }

  /**
    * 从空文件初始化为相应格式的文件
    */
  protected def initEmptyFile(): Unit

  /**
    * 加载文件到内存
    */
  protected def loadFile(): Unit

  /**
    * 关闭文件资源，此操作比较耗时
    */
  override def close(): Unit = {
    mbBuffer.force().clear()
    fc.close()
    raFile.close()
  }

}
