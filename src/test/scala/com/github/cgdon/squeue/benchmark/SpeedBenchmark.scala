package com.github.cgdon.squeue.benchmark

import java.io.File

import com.github.cgdon.jtexttable.TextTable
import com.github.cgdon.squeue.FQueue
import org.apache.commons.io.FileUtils

object SpeedBenchmark extends App {


  val path = args(0)
  val dataFileMb = args(1).toInt

  val rootDir = new File(path)

  val sizeArr = Array.tabulate(4)(x => math.pow(10, x + 4).toInt)

  val seed = ("a" * 1024).getBytes()

  val header = Array("size", "dataFileNum", "cost(s)", "speed(msg/s)", "speed(MB/s)")

  val res = sizeArr.map { size =>
    FileUtils.deleteQuietly(rootDir)
    rootDir.mkdirs()

    val startTs = System.currentTimeMillis()

    val queue = new FQueue(rootDir, dataFileMb)
    for (_ <- 0 until size) {
      queue.offer(seed)
    }
    queue.close()

    val costSecond = (System.currentTimeMillis() - startTs) / 1000.0

    val dataFileNum = FileUtils.listFiles(rootDir, Array("dat"), false).size()
    val writeMsgSpeed = (size / costSecond).formatted("%.2f")
    val writeMsgByteSpeed = (size / 1024.0 / costSecond).formatted("%.2f")
    Array(size.toString, dataFileNum.toString, costSecond.formatted("%.2f"), writeMsgSpeed, writeMsgByteSpeed)
  }

  println(new TextTable(header, res))

}
