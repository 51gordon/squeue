package com.github.cgdon.sfqueue.util

import java.util.concurrent.{ ExecutorService, TimeUnit }

object Utils {
  def closePool(pool: ExecutorService): Unit = {
    pool.shutdown()
    pool.awaitTermination(Int.MaxValue, TimeUnit.DAYS)
  }
}
