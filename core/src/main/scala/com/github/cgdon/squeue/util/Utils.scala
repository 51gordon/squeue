package com.github.cgdon.squeue.util

import java.util.concurrent.{ ExecutorService, TimeUnit }

object Utils {
  def closePool(pool: ExecutorService): Unit = {
    pool.shutdown()
    pool.awaitTermination(Int.MaxValue, TimeUnit.DAYS)
  }
}
