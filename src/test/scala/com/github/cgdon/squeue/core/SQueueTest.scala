package com.github.cgdon.squeue.core

import com.github.cgdon.squeue.QueueTestTrait
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfter, FunSuite }
import org.slf4j.LoggerFactory

@RunWith(classOf[JUnitRunner])
class SQueueTest extends FunSuite with BeforeAndAfter with QueueTestTrait {

  val logger = LoggerFactory.getLogger(classOf[SQueueTest])

  var queue: SQueue = _

  before {
    print("=" * 100 + "\n")
    initQueueEnv()
    queue = new SQueue(rootDir)
    logger.info(s"queue size: ${queue.size()}")
  }

  after {
    queue.close()
    logger.info(s"queue closed")
  }

  test("queue init test") {
    assert(queue.size() === 0)
  }

  test("add 1 msg test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
  }

  test("peek 1 msg test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf = queue.peek()
    assert(queue.size() === 1)
    assert(buf.isDefined)
    assert(new String(buf.get) === "a")
  }

  test("add once, peek twice test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf1 = queue.peek()
    assert(queue.size() === 1)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.peek()
    assert(queue.size() === 1)
    assert(buf2.isDefined)
    assert(new String(buf2.get) === "a")
  }

  test("poll 1 msg test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf = queue.poll()
    assert(queue.size() === 0)
    assert(buf.isDefined)
    assert(new String(buf.get) === "a")
  }

  test("add once, poll twice test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf1 = queue.poll()
    assert(queue.size() === 0)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.poll()
    assert(queue.size() === 0)
    assert(buf2.isEmpty)
  }

  test("add once, peek once, poll once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf1 = queue.peek()
    assert(queue.size() === 1)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.poll()
    assert(queue.size() === 0)
    assert(buf2.isDefined)
    assert(new String(buf2.get) === "a")
  }

  test("add once, poll once, peek once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf1 = queue.poll()
    assert(queue.size() === 0)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.peek()
    assert(queue.size() === 0)
    assert(buf2.isEmpty)
  }

  test("add once, remove once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    val buf1 = queue.poll()
    assert(queue.size() === 0)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    queue.remove()
    assert(queue.size() === 0)
  }

  test("add once, clear test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)

    queue.clear()
    assert(queue.size() === 0)

    val buf1 = queue.peek()
    assert(buf1.isEmpty)

    val buf2 = queue.poll()
    assert(buf2.isEmpty)
  }

  test("add large data test") {
    val seed = ("a" * 1024).getBytes()
    val len = 1024 * 100
    for (_ <- 1 to len) {
      queue.add(seed)
    }
    assert(queue.size() === len)

    val buf1 = queue.peek()
    assert(queue.size() === len)
    assert(buf1.isDefined)
    assert(buf1.get.length === seed.length)

    val buf2 = queue.poll()
    assert(queue.size() === len - 1)
    assert(buf2.isDefined)
    assert(buf2.get.length === seed.length)

    for (_ <- 1 until len) {
      queue.poll()
    }
    assert(queue.size() === 0)
  }

  test("add multi test") {
    queue.add("a".getBytes())
    queue.add("b".getBytes())
    queue.add("c".getBytes())

    assert(new String(queue.poll().get) === "a")
    assert(new String(queue.poll().get) === "b")
    assert(new String(queue.poll().get) === "c")
    assert(queue.size() === 0)
  }

  test("add multi once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(new String(queue.poll().get) === "a")
    assert(new String(queue.poll().get) === "b")
    assert(new String(queue.poll().get) === "c")
    assert(queue.size() === 0)
  }

  test("add 3 once, peek 3 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.peek(3).map(new String(_)).mkString === "abc")
    assert(queue.size() === 3)
  }

  test("add 3 once, poll 3 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.poll(3).map(new String(_)).mkString === "abc")
    assert(queue.size() === 0)
  }

  test("add 3 once, poll 2+1 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.poll(2).map(new String(_)).mkString === "ab")
    assert(queue.size() === 1)
    assert(queue.poll().map(new String(_)).mkString === "c")
    assert(queue.size() === 0)
  }

  test("add 100 once, poll 10 * 10 once test") {
    queue.add((0 until 100).toArray.map("a" + _).map(_.getBytes()))
    for (i <- 0 until 10) {
      logger.info("10 msg: " + queue.poll(10).map(new String(_)).mkString(","))
      assert(queue.size() === 100 - (i + 1) * 10)
    }
  }

  test("add 100 once, peek 10 * 10 once test") {
    queue.add((0 until 100).toArray.map("a" + _).map(_.getBytes()))
    for (i <- 0 until 10) {
      val bufList = queue.peek(10)
      logger.info("10 msg: " + bufList.map(new String(_)).mkString(","))
      queue.remove(bufList.length)
      assert(queue.size() === 100 - (i + 1) * 10)
    }
  }

  test("add 10 once, peek 20 once test") {
    queue.add((0 until 10).toArray.map("a" + _).map(_.getBytes()))
    val bufList = queue.peek(20)
    assert(bufList.length === 10)
    assert(queue.size() === 10)
  }

  test("add 10 once, poll 20 once test") {
    queue.add((0 until 10).toArray.map("a" + _).map(_.getBytes()))
    val bufList = queue.poll(20)
    assert(bufList.length === 10)
    assert(queue.size() === 0)
  }
}