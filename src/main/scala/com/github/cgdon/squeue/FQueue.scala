package com.github.cgdon.squeue

import java.io.File
import java.util.concurrent.locks.{ Lock, ReentrantReadWriteLock }

import com.github.cgdon.squeue.core.SQueue

/**
  * Created by 成国栋 on 2017-11-11 00:22:00.
  */
class FQueue(val dir: File, val dataFileSizeMb: Int = 2) extends java.util.AbstractQueue[Array[Byte]] with AutoCloseable {

  def this(dirPath: String, dataFileSizeMb: Int) {
    this(new File(dirPath), dataFileSizeMb)
  }

  private val queue = new SQueue(dir, dataFileSizeMb)

  private val lock: Lock = new ReentrantReadWriteLock().writeLock()

  override def iterator() = throw new UnsupportedOperationException

  override def size(): Int = {
    val longSize = queue.size()
    if (longSize > Int.MaxValue) {
      throw new IllegalStateException("Size is too large, use longSize() instead!")
    }
    longSize.toInt
  }

  def longSize: Long = queue.size()

  override def poll(): Array[Byte] = {
    lockRun(() => queue.poll().orNull)
  }

  override def offer(buf: Array[Byte]): Boolean = {
    lockRun[Unit](() => queue.add(buf))
    true
  }

  override def peek(): Array[Byte] = {
    lockRun(() => queue.peek().orNull)
  }

  def lockRun[T](func: () => T): T = {
    try {
      lock.lock()
      func()
    } finally {
      lock.unlock()
    }
  }

  override def close(): Unit = {
    lockRun(() => queue.close())
  }
}
