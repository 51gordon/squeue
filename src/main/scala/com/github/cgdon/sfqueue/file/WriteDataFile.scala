package com.github.cgdon.sfqueue.file

import java.io.File
import java.util.concurrent.{ ExecutorService, Executors, TimeUnit }
import com.github.cgdon.sfqueue.util.Utils._

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class WriteDataFile(dir: File, index: Int, initMaxLength: Int) extends DataFile(dir, index, initMaxLength) {

  import java.util.concurrent.ThreadFactory

  var pos: Int = DATA_HEADER_LENGTH

  @volatile var shouldClose = false

  // 每隔一段时间自动flush
  val pool: ExecutorService = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = new Thread(r, "DataFileForceThread")
  })
  pool.submit(new Runnable {
    override def run(): Unit = {
      while (!shouldClose) {
        mbBuffer.force()
      }
    }
  })

  /**
    * 是否已写满
    *
    * @param bufLen 数据字节数
    * @return
    */
  def isFull(bufLen: Int): Boolean = {
    (pos + 4 + bufLen) >= FILE_LIMIT
  }

  /**
    * 写数据
    *
    * @param buf
    */
  def write(buf: Array[Byte]): Unit = {
    // 写数据
    mbBuffer.position(pos)
    mbBuffer.putInt(buf.length)
    mbBuffer.put(buf)
    pos += (4 + buf.length)
  }

  /**
    * 关闭资源
    */
  override def close(): Unit = {
    // 设置close标志位
    shouldClose = true

    // 关闭force线程池
    closePool(pool)

    writeEndPos(pos)

    super.close()
  }
}

