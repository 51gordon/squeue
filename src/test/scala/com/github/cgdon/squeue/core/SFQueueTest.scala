package com.github.cgdon.squeue.core

import java.io.File

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{ BeforeAndAfter, FunSuite }

@RunWith(classOf[JUnitRunner])
class SFQueueTest extends FunSuite with BeforeAndAfter {

  val rootDirPath: String = sys.props("java.io.tmpdir") + "squeue"
  var queue: SFQueue = _

  before {
    val rootDir = new File(rootDirPath)
    rootDir.mkdirs()
    println(s"rootDirPath: $rootDirPath")
    rootDir.listFiles().foreach(_.delete())
    queue = new SFQueue(rootDir)
  }

  after {
    queue.close()
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
  }
}