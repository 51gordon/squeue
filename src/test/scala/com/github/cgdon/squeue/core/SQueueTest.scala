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
    queue = new SQueue(rootDir, 10)
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
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
  }

  test("peek 1 msg test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)

    val buf = queue.peek()
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf.isDefined)
    assert(new String(buf.get) === "a")
  }

  test("add once, peek twice test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.peek()
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.peek()
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf2.isDefined)
    assert(new String(buf2.get) === "a")
  }

  test("poll 1 msg test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf = queue.poll()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf.isDefined)
    assert(new String(buf.get) === "a")
  }

  test("add once, poll twice test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.poll()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.poll()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf2.isEmpty)
  }

  test("add once, peek once, poll once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.peek()
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.poll()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf2.isDefined)
    assert(new String(buf2.get) === "a")
  }

  test("add once, poll once, peek once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.poll()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf1.isDefined)
    assert(new String(buf1.get) === "a")

    val buf2 = queue.peek()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
    assert(buf2.isEmpty)
  }

  test("add once, remove once test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    queue.remove()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)
  }

  test("add once, clear test") {
    queue.add("a".getBytes)
    assert(queue.size() === 1)
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    queue.clear()
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 0)
    assert(queue.totalInSize() === 0)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.peek()
    assert(queue.totalInNum() === 0)
    assert(queue.totalInSize() === 0)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf1.isEmpty)

    val buf2 = queue.poll()
    assert(queue.totalInNum() === 0)
    assert(queue.totalInSize() === 0)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf2.isEmpty)
  }

  test("add large data test") {
    val seed = ("ab" * 1024).getBytes()
    val len = 1024 * 100
    for (_ <- 1 to len) {
      queue.add(seed)
    }
    assert(queue.size() === len)
    assert(queue.totalInNum() === len)
    assert(queue.totalInSize() === len * 2 * 1024)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    val buf1 = queue.peek()
    assert(queue.size() === len)
    assert(queue.totalInNum() === len)
    assert(queue.totalInSize() === len * 2 * 1024)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(buf1.isDefined)
    assert(buf1.get.length === seed.length)

    val buf2 = queue.poll()
    assert(queue.size() === len - 1)
    assert(queue.totalInNum() === len)
    assert(queue.totalInSize() === len * 2 * 1024)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1024 * 2)
    assert(buf2.isDefined)
    assert(buf2.get.length === seed.length)

    for (_ <- 1 until len) {
      queue.poll()
    }
    assert(queue.size() === 0)
    assert(queue.totalInNum() === len)
    assert(queue.totalInSize() === len * 2 * 1024)
    assert(queue.totalOutNum() === len)
    assert(queue.totalOutSize() === len * 2 * 1024)
  }

  test("add multi test") {
    queue.add("a".getBytes())
    assert(queue.totalInNum() === 1)
    assert(queue.totalInSize() === 1)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    queue.add("b".getBytes())
    assert(queue.totalInNum() === 2)
    assert(queue.totalInSize() === 2)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    queue.add("c".getBytes())
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    assert(new String(queue.poll().get) === "a")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 1)
    assert(queue.totalOutSize() === 1)

    assert(new String(queue.poll().get) === "b")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 2)
    assert(queue.totalOutSize() === 2)

    assert(new String(queue.poll().get) === "c")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 3)
    assert(queue.totalOutSize() === 3)
    assert(queue.size() === 0)
  }

  test("add multi once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(new String(queue.poll().get) === "a")
    assert(new String(queue.poll().get) === "b")
    assert(new String(queue.poll().get) === "c")
    assert(queue.size() === 0)
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 3)
    assert(queue.totalOutSize() === 3)
  }

  test("add 3 once, peek 3 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    assert(queue.peek(3).map(new String(_)).mkString === "abc")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(queue.size() === 3)
  }

  test("add 3 once, poll 3 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    assert(queue.poll(3).map(new String(_)).mkString === "abc")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 3)
    assert(queue.totalOutSize() === 3)
    assert(queue.size() === 0)
  }

  test("add 3 once, poll 2+1 once test") {
    queue.add(Array("a", "b", "c").map(_.getBytes()))
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)

    assert(queue.poll(2).map(new String(_)).mkString === "ab")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 2)
    assert(queue.totalOutSize() === 2)

    assert(queue.size() === 1)
    assert(queue.poll().map(new String(_)).mkString === "c")
    assert(queue.totalInNum() === 3)
    assert(queue.totalInSize() === 3)
    assert(queue.totalOutNum() === 3)
    assert(queue.totalOutSize() === 3)
    assert(queue.size() === 0)
  }

  test("add 100 once, poll 10 * 10 once test") {
    queue.add((0 until 100).toArray.map("a" + _).map(_.getBytes()))
    assert(queue.totalInNum() === 100)
    assert(queue.totalInSize() === (0 until 100).map(1 + _.toString.length).sum)
    for (i <- 0 until 10) {
      logger.info("10 msg: " + queue.poll(10).map(new String(_)).mkString(","))
      assert(queue.size() === 100 - (i + 1) * 10)
    }
    assert(queue.totalOutNum() === 100)
    assert(queue.totalOutSize() === (0 until 100).map(1 + _.toString.length).sum)
  }

  test("add 100 once, peek 10 * 10, then remove 10 * 10 test") {
    queue.add((0 until 100).toArray.map("a" + _).map(_.getBytes()))
    assert(queue.totalInNum() === 100)
    assert(queue.totalInSize() === (0 until 100).map(1 + _.toString.length).sum)
    for (i <- 0 until 10) {
      val bufList = queue.peek(10)
      logger.info("10 msg: " + bufList.map(new String(_)).mkString(","))
      queue.remove(bufList.length)
      assert(queue.size() === 100 - (i + 1) * 10)
    }
    assert(queue.totalOutNum() === 100)
    assert(queue.totalOutSize() === (0 until 100).map(1 + _.toString.length).sum)
  }

  test("add 10 once, peek 20 once test") {
    queue.add((0 until 10).toArray.map("a" + _).map(_.getBytes()))
    assert(queue.totalInNum() === 10)
    assert(queue.totalInSize() === (0 until 10).map(1 + _.toString.length).sum)

    val bufList = queue.peek(20)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
    assert(bufList.length === 10)
    assert(queue.size() === 10)
  }

  test("add 10 once, poll 20 once test") {
    queue.add((0 until 10).toArray.map("a" + _).map(_.getBytes()))
    assert(queue.totalInNum() === 10)
    assert(queue.totalInSize() === (0 until 10).map(1 + _.toString.length).sum)

    val bufList = queue.poll(20)
    assert(queue.totalOutNum() === 10)
    assert(queue.totalOutSize() === (0 until 10).map(1 + _.toString.length).sum)
    assert(bufList.length === 10)
    assert(queue.size() === 0)
  }

  test("clear stat data test") {
    queue.add((0 until 10).toArray.map("a" + _).map(_.getBytes()))
    assert(queue.totalInNum() === 10)
    assert(queue.totalInSize() === (0 until 10).map(1 + _.toString.length).sum)

    val bufList = queue.poll(20)
    assert(queue.totalOutNum() === 10)
    assert(queue.totalOutSize() === (0 until 10).map(1 + _.toString.length).sum)
    assert(bufList.length === 10)
    assert(queue.size() === 0)

    queue.clearStatData()
    assert(queue.totalInNum() === 0)
    assert(queue.totalInSize() === 0)
    assert(queue.totalOutNum() === 0)
    assert(queue.totalOutSize() === 0)
  }
}