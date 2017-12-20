package com.github.cgdon.squeue.benchmark

import java.io.File

import com.github.cgdon.jtexttable.TextTable
import com.github.cgdon.squeue.FQueue
import org.apache.commons.io.FileUtils

object SpeedBenchmark extends App {

  val path = args(0)
  val dataFileMb = args(1).toInt

  val rootDir = new File(path)

  val sizeArr = Array.tabulate(3)(x => math.pow(10, x + 4).toInt)

  val seed = ("a" * 1024).getBytes()

  val header = Array("size", "dataFileNum", "ReadCost(s)", "ReadSpeed(msg/s)", "ReadSpeed(MB/s)", "ReadCost(s)", "ReadSpeed(msg/s)", "ReadSpeed(MB/s)")

  val res = sizeArr.map { size =>
    FileUtils.deleteQuietly(rootDir)
    rootDir.mkdirs()

    val queue = new FQueue(rootDir, dataFileMb)

    val t0 = System.currentTimeMillis()
    for (_ <- 0 until size) {
      queue.offer(seed)
    }
    val t1 = System.currentTimeMillis()
    for (_ <- 0 until size) {
      queue.poll()
    }
    val t2 = System.currentTimeMillis()
    queue.close()

    val writeCostSecond = (t1 - t0) / 1000.0
    val readCostSecond = (t2 - t1) / 1000.0

    val dataFileNum = FileUtils.listFiles(rootDir, Array("dat"), false).size()

    val writeMsgSpeed = (size / writeCostSecond).formatted("%.2f")
    val writeMsgByteSpeed = (size / 1024.0 / writeCostSecond).formatted("%.2f")

    val readMsgSpeed = (size / readCostSecond).formatted("%.2f")
    val readMsgByteSpeed = (size / 1024.0 / readCostSecond).formatted("%.2f")
    Array(size.toString, dataFileNum.toString,
      writeCostSecond.formatted("%.2f"), writeMsgSpeed, writeMsgByteSpeed,
      readCostSecond.formatted("%.2f"), readMsgSpeed, readMsgByteSpeed)
  }

  println(new TextTable(header, res))

}
