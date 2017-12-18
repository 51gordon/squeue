package com.github.cgdon.squeue.benchmark

import java.io.File

import com.github.cgdon.squeue.FQueue
import org.apache.commons.io.FileUtils

object SpeedBenchmark extends App {


  val path = args(0)
  val dataFileMb = args(1).toInt
  val rootDir = new File(path)

  val sizeArr = List.tabulate(5)(x => math.pow(10, x + 5).toInt)

  val seed = ("a" * 1024).getBytes()

  val header = Seq("size", "cost(ms)")

  sizeArr.map { size =>
    rootDir.mkdirs()
    val queue = new FQueue(rootDir, dataFileMb)
    val startTs = System.currentTimeMillis()
    for (_ <- 0 until size) {
      queue.add(seed)
    }
    val endTs = System.currentTimeMillis()
    FileUtils.deleteQuietly(rootDir)
    Seq(size,(endTs-startTs))
  }


}
