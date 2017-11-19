package com.github.cgdon.sfqueue.example;

import com.github.cgdon.sfqueue.FQueue;

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

    // 移除队列数据
    String removeData = new String(queue.remove());
    System.out.println("remove data: " + removeData);

    // 清空队列数据
    queue.clear();
    System.out.println("queue size after clear: " + queue.size());

    // 关闭队列
    queue.close();
  }
}
