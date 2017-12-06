package com.github.cgdon.squeue.example

import com.github.cgdon.squeue.FQueue

object ScalaExample extends App {

  // 声明一个Queue
  val queue = new FQueue("/tmp/queue", 10)

  // 向队列添加数据
  queue.offer("abc".getBytes())
  queue.offer("abcd".getBytes())

  // 获取队列大小(如果超过Int.MaxValue会抛出异常，可以使用longSize替代)
  println(s"queue int size: ${queue.size()}")

  // 获取队列大小
  println(s"queue long size: ${queue.longSize}")

  // 获取一条数据，但是不移除
  val peekData = new String(queue.peek())
  println(s"peek data: $peekData")

  // 获取一条数据，并移除之
  val pollData = new String(queue.poll())
  println(s"poll data: $pollData")

  // 移除队列数据
  val removeData = new String(queue.remove())
  println(s"remove data: $removeData")

  // 清空队列数据
  queue.clear()
  println(s"queue size after clear: ${queue.size()}")

  // 关闭队列
  queue.close()
}
