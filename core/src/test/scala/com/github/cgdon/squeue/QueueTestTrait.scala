package com.github.cgdon.squeue

import java.io.File

import org.slf4j.LoggerFactory

trait QueueTestTrait {

  private val logger = LoggerFactory.getLogger(classOf[QueueTestTrait])

  val rootDir = new File(sys.props("java.io.tmpdir"), "squeue")

  def initQueueEnv(): Unit = {
    rootDir.mkdirs()
    logger.info(s"queueDir: $rootDir")
    rootDir.listFiles().foreach(_.delete())
  }

}
