package com.github.cgdon.squeue

import java.util.concurrent.Executors

import com.github.cgdon.squeue.util.Utils
import org.scalatest.{ BeforeAndAfter, FunSuite }

class FQueueTest extends FunSuite with BeforeAndAfter with QueueTestTrait {

  var queue: FQueue = _

  before {
    initQueueEnv()
    queue = new FQueue(rootDir, 512)
  }

  after {
    queue.close()
  }

  test("daemon test") {
    val start = System.currentTimeMillis()
    val len = 10000 * 10
    val seed = ("a" * 1024).getBytes()
    val pool = Executors.newFixedThreadPool(2)
    pool.submit(new Runnable {
      override def run(): Unit = {
        for (_ <- 1 to len) {
          queue.offer(seed)
        }
      }
    })
    pool.submit(new Runnable {
      override def run(): Unit = {
        var count: Int = 0
        @volatile var shouldStop = false
        while (!shouldStop) {
          val buf = Option(queue.poll())
          if (buf.isDefined) {
            count += 1
          }
          if (count >= len) {
            shouldStop = true
          }
        }
      }
    })
    Utils.closePool(pool)
    val end = System.currentTimeMillis()
    println(s"cost: ${end - start}ms")
    assert(queue.size() === 0)
  }
}
