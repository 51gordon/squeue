package com.github.cgdon.squeue.example;

import com.github.cgdon.squeue.FQueue;

import java.io.File;

public class JavaExample {
  public static void main(String[] args) {
    // 声明一个Queue
    File rootDir = new File(System.getProperty("java.io.tmpdir"), "queue");
    rootDir.mkdirs();
    FQueue queue = new FQueue(rootDir, 10);

    // 向队列添加数据
    for (int i = 0; i < 10; i++) {
      queue.offer(("abc" + i).getBytes());
    }

    // 获取队列大小(如果超过Int.MaxValue会抛出异常，可以使用longSize替代)
    System.out.println("queue int size: " + queue.size());

    // 获取队列大小
    System.out.println("queue long size: " + queue.longSize());

    // 获取一条数据，但是不移除
    String peekData = new String(queue.peek());
    System.out.println("peek data: " + peekData);

    // 获取最多3条数据，但是不移除
    byte[][] peekDataList = queue.peek(3);
    System.out.println("peek data length: " + peekDataList.length);
    for (byte[] bs : peekDataList) {
      System.out.println("peek data: " + new String(bs));
    }

    // 获取一条数据，并移除之
    String pollData = new String(queue.poll());
    System.out.println("poll data: " + pollData);
    System.out.println("queue size after poll: " + queue.size());

    // 获取最多3条数据，并移除之
    byte[][] pollDataList = queue.poll(3);
    System.out.println("poll(3) data length: " + pollDataList.length);
    for (byte[] bs : pollDataList) {
      System.out.println("poll data: " + new String(bs));
    }
    System.out.println("queue size after poll(3): " + queue.size());

    // 移除队列数据
    String removeData = new String(queue.remove());
    System.out.println("remove data: " + removeData);
    System.out.println("queue size after remove: " + queue.size());

    // 删除队列头部的4条数据，并且不返回移除的数据
    queue.remove(3);
    System.out.println("queue size after remove(3): " + queue.size());

    // 队列总计添加过多少条数据
    System.out.println("queue total add record num: " + queue.totalInNum());

    // 队列总计添加过的数据的字节数
    System.out.println("queue total add record byte size: " + queue.totalInSize());

    // 队列总计删除过多少条数据
    System.out.println("queue total delete record num: " + queue.totalOutNum());

    // 队列总计删除过的数据的字节数
    System.out.println("queue total delete record byte size: " + queue.totalOutSize());

    // 清空队列数据及统计信息
    queue.clear();
    System.out.println("queue size after clear: " + queue.size());

    // 关闭队列
    queue.close();
  }
}
