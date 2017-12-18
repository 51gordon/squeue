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

  val header = Array("size", "cost(s)", "speed(msg/s)")

  val res = sizeArr.map { size =>
    FileUtils.deleteQuietly(rootDir)
    rootDir.mkdirs()
    val queue = new FQueue(rootDir, dataFileMb)
    val startTs = System.currentTimeMillis()
    for (_ <- 0 until size) {
      queue.add(seed)
    }
    val costSecond = (System.currentTimeMillis() - startTs) / 1000.0
    queue.close()
    Array(size.toString, costSecond.formatted("%.2f"), (size / costSecond).formatted("%.2f"))
  }

  println(new TextTable(header, res))

}
