### 简介 

SFQueue是FQueue的scala版本，是一个高性能、基于磁盘持久存储的嵌入式队列消息系统(Scala Fast Queue)，采用Scala语言开发，可以与Java程序无缝集成。


### 特性 

  * 基于磁盘持久化存储，数据无限存储，只受限于磁盘空间。
  * 高性能，能达到数十万qps。
  * 低内存消耗。
  * 高效率IO读写算法，IO效率高。
  * 纯Scala代码，支持进程内JVM级别的直接调用。
  
###  Benchmark
  待补充

###  例子 
  Scala:
  ```
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
      
        // 移除队列一条数据
        val removeData = new String(queue.remove())
        println(s"remove data: $removeData")
      
        // 清空队列数据
        queue.clear()
        println(s"queue size after clear: ${queue.size()}")
      
        // 关闭队列
        queue.close()
      }
   ```
   
   Java
   ```
      package com.github.cgdon.squeue.example;
      
      import com.github.cgdon.squeue.FQueue;
      
      public class JavaExample {
        public static void main(String[] args) {
          // 声明一个Queue
          FQueue queue = new FQueue("/tmp/queue", 10);
      
          // 向队列添加数据
          queue.offer("abc".getBytes());
          queue.offer("abcd".getBytes());
      
          // 获取队列大小(如果超过Int.MaxValue会抛出异常，可以使用longSize替代)
          System.out.println("queue int size: " + queue.size());
      
          // 获取队列大小
          System.out.println("queue long size: " + queue.longSize());
      
          // 获取一条数据，但是不移除
          String peekData = new String(queue.peek());
          System.out.println("peek data: " + peekData);
      
          // 获取一条数据，并移除之
          String pollData = new String(queue.poll());
          System.out.println("poll data: " + pollData);
      
          // 移除队列一条数据
          String removeData = new String(queue.remove());
          System.out.println("remove data: " + removeData);
      
          // 清空队列数据
          queue.clear();
          System.out.println("queue size after clear: " + queue.size());
      
          // 关闭队列
          queue.close();
        }
      }
   ```
