package com.github.cgdon.sfqueue.core

import java.io.File

import org.specs2.mutable.Specification
import org.specs2.specification.{ AfterEach, BeforeEach }

class SFQueueTest extends Specification with BeforeEach with AfterEach {

  val rootDir = "/tmp/sfqueue"

  var queue: SFQueue = _

  val seed1k = "Scala编程令式”或者“函数式”。Scalaa一个新的语言品种，它抹平了这些人为划分的界限。\n根据David Rupp在博客中的说法，Scala可能是下一代Java。这么高的评价让人不禁想看看它到底是什么东西。\nScala有几项关键特性表明了它的面向对象的本质。例如，Scala中的每个值都是一个对象，包括基本数据类型（即布尔值、数字等）在内，连函数也是对象。另外，类可以被子类化，而且Scala还提供了基于mixin的组合（mixin-based composition）。\n与只支持单继承的语言相比，Scala具有更广泛意义上的类重用。Scala允许定义新类的时候重用“一个类中新增的成员定义（即相较于其父类的差异之处）”。Scala称之为mixin类组合。\nScala还包含了若干函数式语言的关键概念，包括高阶函数（Higher-Order Function）、局部套用（Currying）、嵌套函数（Nested Function）、序列解读（Sequence Comprehensions）等等。".getBytes

  override protected def before: Any = {
    val root = new File(rootDir)
    root.mkdirs()
    root.listFiles().foreach(_.delete())
    println("before")
    queue = new SFQueue(rootDir)
  }

  override protected def after: Any = {
    println("after")
    queue.close()
  }


  "SFQueue" should {

    "init ok" in {
      println(s"queue: $queue")
      println(s"queue.size: ${queue.size()}")
      ok
    }

    "add one ok" in {
      queue.add("abc".getBytes)
      println(s"queue.size: ${queue.size()}")
      assert(queue.size() == 1)
      ok
    }

    "peek one ok" in {
      queue.add("abc".getBytes)
      println(s"after add queue.size: ${queue.size()}")

      val s1 = new String(queue.peek().get)
      println(s"after peek queue.size: ${queue.size()}")
      assert(s1 == "abc")
      assert(queue.size() == 1)

      val s2 = new String(queue.poll().get)
      println(s"after poll queue.size: ${queue.size()}")
      assert(s2 == "abc")
      assert(queue.size() == 0)
      ok
    }

    "poll one ok" in {
      queue.add("abc".getBytes)
      println(s"after add queue.size: ${queue.size()}")

      val s = new String(queue.poll().get)
      println(s"after poll queue.size: ${queue.size()}")
      assert(s == "abc")
      assert(queue.size() == 0)
      ok
    }

    "remove one ok" in {
      queue.add("abc".getBytes)
      println(s"after add queue.size: ${queue.size()}")

      queue.remove()
      println(s"after remove queue.size: ${queue.size()}")
      assert(queue.size() == 0)
      ok
    }

    "add multi msg ok" in {
      println(s"seed len: ${seed1k.length}")
      val len = 1024 * 10
      for (i <- 1 to len) {
        queue.add(seed1k)
      }
      println(s"queue.size: ${queue.size()}")
      assert(queue.size() == 10240)
      ok
    }

    "poll multi msg ok" in {
      val len = 1024 * 10
      for (_ <- 1 to len) {
        queue.add(seed1k)
      }
      println(s"queue.size: ${queue.size()}")
      assert(queue.size() == len)

      for (i <- 1 to len) {
        val buf = queue.poll()
        assert(buf.get.length == 1024)
      }
      println(s"queue.size: ${queue.size()}")
      assert(queue.size() == 0)
      ok
    }
  }
}
