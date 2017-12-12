package com.github.cgdon.squeue.example

import java.io.File

import com.github.cgdon.squeue.FQueue

object ScalaExample extends App {

  // 声明一个Queue
  val rootDir = new File(System.getProperty("java.io.tmpdir"), "queue")
  rootDir.mkdirs()
  val queue = new FQueue(rootDir, 10)

  // 向队列添加数据
  for (i <- 0 until 10) {
    queue.offer(("abc" + i).getBytes())
  }

  // 获取队列大小(如果超过Int.MaxValue会抛出异常，可以使用longSize替代)
  println("queue int size: " + queue.size())

  // 获取队列大小
  System.out.println("queue long size: " + queue.longSize())

  // 获取一条数据，但是不移除
  val peekData = new String(queue.peek())
  println("peek data: " + peekData)

  // 获取最多3条数据，但是不移除
  val peekDataList = queue.peek(3)
  println("peek data length: " + peekDataList.length)
  for (bs <- peekDataList) {
    println("peek data: " + new String(bs))
  }

  // 获取一条数据，并移除之
  val pollData = new String(queue.poll())
  println("poll data: " + pollData)
  println("queue size after poll: " + queue.size())

  // 获取最多3条数据，并移除之
  val pollDataList = queue.poll(3)
  println("poll(3) data length: " + pollDataList.length)
  for (bs <- pollDataList) {
    println("poll data: " + new String(bs))
  }
  println("queue size after poll(3): " + queue.size())

  // 移除队列数据
  val removeData = new String(queue.remove())
  println("remove data: " + removeData)
  println("queue size after remove: " + queue.size())

  // 删除队列头部的4条数据，并且不返回移除的数据
  queue.remove(3)
  println("queue size after remove(3): " + queue.size())

  // 清空队列数据
  queue.clear()
  println("queue size after clear: " + queue.size())

  // 队列总计添加过多少条数据
  println("queue total add record num: " + queue.totalInNum)

  // 队列总计添加过的数据的字节数
  println("queue total add record byte size: " + queue.totalInSize)

  // 队列总计删除过多少条数据
  println("queue total delete record num: " + queue.totalOutNum)

  // 队列总计删除过的数据的字节数
  println("queue total delete record byte size: " + queue.totalOutSize)

  // 关闭队列
  queue.close()
}
