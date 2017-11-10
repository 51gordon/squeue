package com.github.cgdon.sfqueue.file

import java.io.File

/**
  * queue文件接口
  * Created by 成国栋 on 2017-11-11 00:44:00.
  */
trait QueueFile {

  /**
    * 返回该文件的magic
    *
    * @return
    */
  def magic(): String

  def init(f: File): Unit = {
    if (!f.exists()) {
      f.createNewFile()
      // 初始化索引文件
      initFile()
    }

    // 加载索引文件
    loadFile()
  }

  /**
    * 初始化文件
    */
  def initFile(): Unit

  /**
    * 加载文件
    */
  def loadFile(): Unit

}
