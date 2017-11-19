package com.github.cgdon.sfqueue

import java.util
import java.util.AbstractList
import java.util.concurrent.locks.{ Lock, ReentrantReadWriteLock }

import com.github.cgdon.sfqueue.core.SFQueue

/**
  * Created by 成国栋 on 2017-11-11 00:22:00.
  */
class FQueue(val dirPath: String) extends util.AbstractQueue[Array[Byte]] {

  private val queue = new SFQueue(dirPath)

  private val lock: Lock = new ReentrantReadWriteLock().writeLock()

  override def iterator() = throw new UnsupportedOperationException

  override def size() = {
    val longSize = queue.size()
    if (longSize > Int.MaxValue) {
      throw new IllegalStateException("Size is too large, use longSize() instead!")
    }
    longSize.toInt
  }

  def longSize = queue.size()

  override def poll(): Array[Byte] = {
    lockRun(() => queue.poll().orNull)
  }

  override def offer(buf: Array[Byte]) = {
    lockRun[Unit](() => queue.add(buf))
    true
  }

  override def peek() = {
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
}
