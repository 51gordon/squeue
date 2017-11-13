package com.github.cgdon.sfqueue.core

import java.io.File

import org.specs2.mutable.Specification
import org.specs2.specification.{ AfterEach, BeforeEach }

class SFQueueTest extends Specification with BeforeEach with AfterEach {

  val rootDir = "/tmp/sfqueue"

  var queue: SFQueue = _

  override protected def before: Any = {
    val root = new File(rootDir)
    root.listFiles().foreach(_.delete())
    queue = new SFQueue(rootDir)
  }

  override protected def after: Any = {

  }


  "SFQueue" should {

    "init ok" in {
      println(s"queue: $queue")
      println(s"queue.size: ${queue.size()}")
      ok
    }

    "offer ok" in {
      queue.offer("abc".getBytes)
      println(s"queue.size: ${queue.size()}")
      assert(queue.size() == 1)
      ok
    }

    "poll ok" in {
      queue.offer("abc".getBytes)
      println(s"after offer queue.size: ${queue.size()}")

      val s = new String(queue.poll().get)
      println(s"after poll queue.size: ${queue.size()}")
      assert(s == "abc")
      assert(queue.size() == 0)
      ok
    }
  }
}
