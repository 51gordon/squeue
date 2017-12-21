package com.github.cgdon.squeue.benchmark

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.github.cgdon.squeue.FQueue
import org.apache.commons.io.FileUtils

object QueueSizeBenchmark extends App {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val path = args(0)
  val dataFileMb = args(1).toInt
  val len = args(2).toInt
  val logBatch = args(3).toInt

  val rootDir = new File(path)

  val seed = ("a" * 120).getBytes()

  FileUtils.deleteQuietly(rootDir)
  rootDir.mkdirs()

  val queue = new FQueue(rootDir, dataFileMb)

  val t0 = System.currentTimeMillis()
  for (i <- 0 until 3) {
    for (j <- 0 until len) {
      queue.offer(seed)
      if (j > 0 && j % logBatch == 0) {
        val fileSize = FileUtils.listFiles(rootDir, Array("dat"), false).size()
        info(s"Write, fileSize: $fileSize i: $i,j: $j")
        info(s"Write, queue.longSize: ${queue.longSize()}")
      }
    }
  }
  val t1 = System.currentTimeMillis()
  info(s"Total write ${3 * len}, cost ${(t1 - t0) / 1000}s")
  info(s"After write queue.longSize: ${queue.longSize()}")
  for (i <- 0 until 3) {
    for (j <- 0 until len) {
      queue.poll()
      if (j > 0 && j % logBatch == 0) {
        val fileSize = FileUtils.listFiles(rootDir, Array("dat"), false).size()
        info(s"Read, fileSize: $fileSize i: $i,j: $j")
        info(s"Read, queue.longSize: ${queue.longSize()}")
      }
    }
  }
  info(s"After read, queue.longSize: ${queue.longSize()}")
  val t2 = System.currentTimeMillis()
  info(s"Total read ${3 * len}, cost ${(t2 - t1) / 1000}s")
  queue.close()

  def info(s: String): Unit = println(sdf.format(new Date()) + " " + s)

}
