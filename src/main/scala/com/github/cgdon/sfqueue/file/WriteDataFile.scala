package com.github.cgdon.sfqueue.file

import java.io.File
import java.util.concurrent.{ ExecutorService, Executors, TimeUnit }

/**
  * Created by 成国栋 on 2017-11-11 00:34:00.
  */
class WriteDataFile(dir: File, index: Int, initMaxLength: Int) extends DataFile(dir, index, initMaxLength) {

  var writePos: Int = DATA_HEADER_LENGTH

  // 没10ms自动flush
  val pool: ExecutorService = Executors.newFixedThreadPool(1)
  pool.submit(new Runnable {
    override def run(): Unit = {
      while (true) {
        mbBuffer.force()
      }
    }
  })

  def available(buf: Array[Byte]): Int = {
    FILE_LIMIT - (writePos + 4 + buf.length)
  }

  def write(buf: Array[Byte]): Int = {
    val delta = 4 + buf.length
    mbBuffer.position(writePos)
    mbBuffer.putInt(buf.length)
    mbBuffer.put(buf)
    writePos += delta
    delta
  }

  override def close(): Unit = {
    pool.shutdown()
    pool.awaitTermination(10, TimeUnit.SECONDS)
    super.close()
  }
}

