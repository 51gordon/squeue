package com.github.cgdon.squeue

import java.io.File
import java.util.concurrent.locks.{ Lock, ReentrantReadWriteLock }

import com.github.cgdon.squeue.core.SQueue

/**
  * Created by 成国栋 on 2017-11-11 00:22:00.
  */
class FQueue(val dir: File, val dataFileSizeMb: Int) extends java.util.AbstractQueue[Array[Byte]] with AutoCloseable {

  def this(dirPath: String, dataFileSizeMb: Int) {
    this(new File(dirPath), dataFileSizeMb)
  }

  private val queue = new SQueue(dir, dataFileSizeMb)

  private val lock: Lock = new ReentrantReadWriteLock().writeLock()

  override def iterator() = throw new UnsupportedOperationException

  def totalInNum() = queue.totalInNum()

  def totalOutNum() = queue.totalOutNum()

  def totalInSize() = queue.totalInSize()

  def totalOutSize() = queue.totalOutSize()

  override def size(): Int = {
    val longSize = queue.size()
    if (longSize > Int.MaxValue) {
      throw new IllegalStateException("Size is too large, use longSize() instead!")
    }
    longSize.toInt
  }

  def longSize(): Long = queue.size()


  override def offer(buf: Array[Byte]): Boolean = {
    checkBuf(buf)
    lockRun[Unit](() => queue.add(buf))
    true
  }

  def offer(bufList: Array[Array[Byte]]): Boolean = {
    if (bufList == null) throw new NullPointerException()
    bufList.foreach(buf => checkBuf(buf))
    lockRun[Unit](() => queue.add(bufList))
    true
  }

  private def checkBuf(buf: Array[Byte]): Unit = {
    if (buf == null) throw new NullPointerException()
    if (buf.length >= (dataFileSizeMb << 20)) throw new IllegalArgumentException(s"Data too large, max length is ${dataFileSizeMb << 20}")
  }

  override def peek(): Array[Byte] = {
    lockRun(() => queue.peek().orNull)
  }

  def peek(maxNum: Int): Array[Array[Byte]] = {
    if (maxNum <= 0) throw new IllegalArgumentException("maxNum must be > 0")
    lockRun(() => queue.peek(maxNum))
  }

  override def poll(): Array[Byte] = {
    lockRun(() => queue.poll().orNull)
  }

  def poll(maxNum: Int): Array[Array[Byte]] = {
    if (maxNum <= 0) throw new IllegalArgumentException("maxNum must be > 0")
    lockRun(() => queue.poll(maxNum))
  }

  override def remove() = poll()

  def remove(num: Int): Unit = {
    if (num <= 0) throw new IllegalArgumentException("num must be > 0")
    lockRun(() => queue.remove(num))
  }

  override def clear(): Unit = queue.clear()

  def clearStatData(): Unit = queue.clear()

  override def close(): Unit = {
    lockRun(() => queue.close())
  }

  def lockRun[T](func: () => T): T = {
    try {
      lock.lock()
      func()
    } finally {
      lock.unlock()
    }
  }

}
