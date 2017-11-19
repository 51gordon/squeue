package com.github.cgdon.sfqueue.core

import java.io.{ File, FilenameFilter }
import java.util.concurrent.{ ExecutorService, Executors, LinkedBlockingQueue }

import com.github.cgdon.sfqueue.file._
import com.github.cgdon.sfqueue.util.Utils._

/**
  * 线程非安全
  * Created by 成国栋 on 2017-11-11 00:26:00.
  */
class SFQueue(dir: File, maxDataFileLength: Int = 1 << 21) {

  val pool: ExecutorService = Executors.newSingleThreadExecutor()

  var readHandler: ReadDataFile = _
  var writeHandler: WriteDataFile = _

  dir.mkdirs()
  // 初始化索引文件和待处理的数据文件
  val idxFile = new IndexFile(dir)
  initDataFiles()

  def this(dirPath: String) = {
    this(new File(dirPath))
  }

  def add(buf: Array[Byte]): Unit = {
    if (writeHandler.isFull(buf.length)) {
      rotateWriteFile()
    }
    writeHandler.write(buf)
    idxFile.incrementSize(buf.length)
  }

  def runHandler[T](f: () => T): Option[T] = {
    if (idxFile.readIdx == idxFile.writeIdx) {
      // 读写同一个文件
      if (idxFile.readPos < idxFile.writePos) Some(f()) else None
    } else if (idxFile.readIdx < idxFile.writeIdx) {
      // 读文件落后于写文件
      if (idxFile.readPos < readHandler.readEndPos()) {
        // 当前文件还没有读完
        Some(f())
      } else {
        // 当前文件已经读完了，滚动到下一个文件，继续读
        rotateReadFile()
        Some(f())
      }
    } else {
      throw new IllegalStateException("Read index > write index")
    }
  }

  def readNext(): Option[Array[Byte]] = {
    runHandler(() => readHandler.readNext())
  }

  def peek(): Option[Array[Byte]] = {
    readNext()
  }

  def poll(): Option[Array[Byte]] = {
    val bufOpt = readNext()
    if (bufOpt.isDefined) {
      idxFile.decrementSize(bufOpt.get.length)
    }
    bufOpt
  }

  def remove(): Unit = {
    runHandler(() => {
      val len = readHandler.remove()
      idxFile.decrementSize(len)
    })
  }

  def size(): Long = idxFile.size()

  /**
    * 清空文件队列
    */
  def clear(): Unit = {
    // 清空
    //    idxFile.clear()
    //    initDataFiles()
    throw new UnsupportedOperationException()
  }

  /**
    * 关闭文件队列
    */
  def close(): Unit = {
    writeHandler.close()
    readHandler.close()
    idxFile.close()
    closePool(pool)
    cleanFile()
  }

  private def initDataFiles(): Unit = {
    readHandler = new ReadDataFile(dir, idxFile.readIdx, maxDataFileLength)
    writeHandler = new WriteDataFile(dir, idxFile.writeIdx, maxDataFileLength)
  }

  private def rotateReadFile(): Unit = {
    idxFile.readIdx += 1
    idxFile.resetReadPos()
    readHandler.close()
    pool.submit(new Runnable {
      override def run(): Unit = {
        readHandler.datFile.delete()
      }
    })
    readHandler = new ReadDataFile(dir, idxFile.readIdx, maxDataFileLength)
  }

  private def rotateWriteFile(): Unit = {
    idxFile.writeIdx += 1
    idxFile.resetWritePos()

    // 关闭旧的 write handler
    writeHandler.close()

    writeHandler = new WriteDataFile(dir, idxFile.writeIdx, maxDataFileLength)
  }

  /**
    * 清理已经读取完的文件
    */
  def cleanFile(): Unit = {
    val filter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = DataFile.isDataFile(name)
    }
    dir.listFiles(filter).foreach { f =>
      val index = DataFile.getIndexByFileName(f.getName)
      if (index < idxFile.readIdx) f.delete()
    }
  }


}
