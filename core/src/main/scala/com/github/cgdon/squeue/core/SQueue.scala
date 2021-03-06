package com.github.cgdon.squeue.core

import java.io.{ File, FilenameFilter }
import java.util.concurrent.{ ExecutorService, Executors }

import com.github.cgdon.squeue.file._
import com.github.cgdon.squeue.util.Utils._

/**
  * 线程非安全
  * Created by 成国栋 on 2017-11-11 00:26:00.
  */
class SQueue(dir: File, dataFileSizeMb: Int) {

  (!dir.exists(), dir.isFile) match {
    case (true, _) =>
      throw new IllegalArgumentException(s"Directory $dir not exist!")
    case (_, true) =>
      throw new IllegalArgumentException(s"$dir not a directory!")
    case (_, _) =>
  }

  // 初始化索引文件和待处理的数据文件
  val idxFile = new IndexFile(dir)
  var readHandler = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb)
  var writeHandler = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb)

  lazy private val pool: ExecutorService = Executors.newSingleThreadExecutor()

  def this(dirPath: String, dataFileSizeMb: Int) = {
    this(new File(dirPath), dataFileSizeMb)
  }

  def totalInNum() = idxFile.totalInRecordNum

  def totalInSize() = idxFile.totalInRecordSize

  def totalOutNum() = idxFile.totalOutRecordNum

  def totalOutSize() = idxFile.totalOutRecordSize

  def add(buf: Array[Byte]): Unit = {
    if (writeHandler.isFull(buf)) {
      rotateWriteFile()
    }
    writeHandler.write(buf)
    idxFile.incrementSize(buf.length)
  }

  //  def add(bufList: Array[Array[Byte]]): Unit = {
  //    if (writeHandler.isFull(bufList)) {
  //      rotateWriteFile()
  //    }
  //    writeHandler.write(bufList)
  //    idxFile.incrementSize(bufList.length, bufList.map(_.length).sum)
  //  }

  def add(bufList: Array[Array[Byte]]): Unit = {
    for (buf <- bufList) {
      if (writeHandler.isFull(buf)) {
        rotateWriteFile()
      }
      writeHandler.write(buf)
    }
    idxFile.incrementSize(bufList.length, bufList.map(_.length).sum)
  }

  private def runHandler[T](f: () => T): Option[T] = {
    idxFile.readIdx - idxFile.writeIdx match {
      case delta: Int if delta == 0 => // 读写同一个文件
        if (idxFile.readPos < idxFile.writePos) Some(f()) else None
      case delta: Int if delta < 0 => // 读文件落后于写文件
        if (idxFile.readPos >= readHandler.readEndPosFromFile()) rotateReadFile() // 如果当前文件已经读完，那么滚动到下一个文件继续读
        Some(f())
      case delta: Int if delta > 0 => // 这种情况不可能出现
        throw new IllegalStateException("Read index > write index, this is a bug!")
    }
  }

  private def readNext(commit: Boolean): Option[Array[Byte]] = {
    runHandler(() => readHandler.readNext(commit))
  }

  private def readData(maxNum: Int, commit: Boolean): Array[Array[Byte]] = {
    size() match {
      case i if i == 0 => Array()
      case i if i > 0 =>
        val min = math.min(i, maxNum.toLong).toInt
        runHandler(() => readHandler.readData(min, commit)).get
    }
  }

  /**
    * 获取一条数据，不从队列删除
    *
    * @return
    */
  def peek(): Option[Array[Byte]] = {
    readNext(false)
  }

  /**
    *
    * @param maxNum 一次性获取maxNum条记录，不从队列删除
    * @return
    */
  def peek(maxNum: Int): Array[Array[Byte]] = {
    readData(maxNum, false)
  }

  /**
    * 获取一条数据，并从队列删除
    *
    * @return
    */
  def poll(): Option[Array[Byte]] = {
    val bufOpt = readNext(true)
    if (bufOpt.isDefined) {
      idxFile.decrementSize(bufOpt.get.length)
    }
    bufOpt
  }

  /**
    *
    * @param maxNum 一次性pool的记录条数
    * @return
    */
  def poll(maxNum: Int): Array[Array[Byte]] = {
    val bufList = readData(maxNum, true)
    if (bufList.length > 0) {
      idxFile.decrementSize(bufList.length, bufList.map(_.length).sum)
    }
    bufList
  }

  /**
    * 移除队列下一个元素
    */
  def remove(): Unit = {
    runHandler(() => {
      val len = readHandler.remove()
      idxFile.decrementSize(len)
    })
  }

  /**
    *
    * @param num 一次性remove的记录条数
    * @return
    */
  def remove(num: Int): Unit = {
    runHandler(() => {
      val len = readHandler.removeData(num)
      idxFile.decrementSize(num, len)
    })
  }


  /**
    * 获取队列数据条数
    *
    * @return
    */
  def size(): Long = idxFile.size()

  /**
    * 清空文件队列
    */
  def clear(): Unit = {
    idxFile.clear()

    readHandler.close()
    writeHandler.close()

    cleanAllDataFile()

    readHandler = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb)
    writeHandler = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb)
  }

  def clearStatData() = idxFile.clearStatData()

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

  private def rotateReadFile(): Unit = {
    idxFile.readIdx += 1
    idxFile.resetReadPos()
    readHandler.close()
    pool.submit(new Runnable {
      override def run(): Unit = {
        readHandler.datFile.delete()
      }
    })
    readHandler = new ReadDataFile(dir, idxFile.readIdx, dataFileSizeMb)
  }

  private def rotateWriteFile(): Unit = {
    idxFile.writeIdx += 1
    idxFile.resetWritePos()

    // 关闭旧的 write handler
    writeHandler.close()

    writeHandler = new WriteDataFile(dir, idxFile.writeIdx, dataFileSizeMb)
  }

  private val datFilter = new FilenameFilter {
    override def accept(dir: File, name: String): Boolean = DataFile.isDataFile(name)
  }

  /**
    * 清理已经读取完的文件
    */
  private def cleanFile(): Unit = {
    dir.listFiles(datFilter).foreach { f =>
      val index = DataFile.getIndexByFileName(f.getName)
      if (index < idxFile.readIdx) f.delete()
    }
  }

  /**
    * 清理所有数据文件
    */
  private def cleanAllDataFile(): Unit = {
    dir.listFiles(datFilter).foreach(_.delete())
  }
}
