package com.github.cgdon.squeue

import java.io.File

import org.scalatest.{ BeforeAndAfter, FunSuite }

class FQueue2Test extends FunSuite with BeforeAndAfter {

  val rootDirPath: String = sys.props("java.io.tmpdir") + "squeue"

  before {
    val rootDir = new File(rootDirPath)
    rootDir.mkdirs()
    println(s"rootDirPath: $rootDirPath")
    rootDir.listFiles().foreach(_.delete())
  }

  after {
  }

  test("rewrite test") {
    val queue1 = new FQueue(rootDirPath, 10)
    queue1.offer("abc".getBytes)
    queue1.close()
    assert(queue1.size() === 1)

    val queue2 = new FQueue(rootDirPath, 10)
    queue2.offer("abcd".getBytes)
    assert(queue2.size() === 2)
    queue2.close()
  }
}
