package com.github.cgdon.squeue

import org.scalatest.{ BeforeAndAfter, FunSuite }

class FQueue2Test extends FunSuite with BeforeAndAfter with QueueTestTrait {

  before {
    initQueueEnv()
    rootDir.listFiles().foreach(_.delete())
  }

  after {
  }

  test("rewrite test") {
    val queue1 = new FQueue(rootDir, 10)
    queue1.offer("abc".getBytes)
    queue1.close()
    assert(queue1.size() === 1)

    val queue2 = new FQueue(rootDir, 10)
    queue2.offer("abcd".getBytes)
    assert(queue2.size() === 2)
    queue2.close()
  }
}
