package xyz.sourcecodestudy.rpc.util

import java.util.concurrent.{ThreadFactory, ThreadPoolExecutor, Executors, ExecutorService}
import java.util.concurrent.{TimeUnit, LinkedBlockingDeque, AbstractExecutorService, RejectedExecutionException}
import java.util.concurrent.{ScheduledExecutorService, ScheduledThreadPoolExecutor}
import java.util.concurrent.locks.ReentrantLock

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

object ThreadUtils {

  def sameThread: ExecutionContextExecutor = {
    ExecutionContext.fromExecutorService(new AbstractExecutorService {

      private val lock = new ReentrantLock()
      private val termination = lock.newCondition()
      private var runningTasks = 0
      private var servicesIsShutdown = false

      override def shutdown(): Unit = {
        lock.lock()
        try {
          servicesIsShutdown = true
        } finally {
          lock.unlock()
        }
      }

      override def shutdownNow(): java.util.List[Runnable] = {
        shutdown()
        java.util.Collections.emptyList()
      }

      override def isShutdown(): Boolean = {
        lock.lock()
        try {
          servicesIsShutdown
        } finally {
          lock.unlock()
        }
      }

      override def isTerminated(): Boolean = {
        lock.lock()
        try {
          servicesIsShutdown && runningTasks == 0
        } finally {
          lock.unlock()
        }
      }

      override def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = {
        var nanos = unit.toNanos(timeout)
        lock.lock()
        try {
          while (nanos > 0 && !isTerminated()) {
            nanos = termination.awaitNanos(nanos)
          }
          isTerminated()
        } finally {
          lock.unlock()
        }
      }

      override def execute(command: Runnable): Unit = {
        lock.lock()
        try {
          if (isShutdown()) throw new RejectedExecutionException("Executor already shutdown")
          runningTasks += 1
        } finally {
          lock.unlock()
        }

        try {
          command.run()
        } finally {
          lock.lock()
          try {
            runningTasks -= 1
            if (isTerminated()) termination.signalAll()
          } finally {
            lock.unlock()
          }
        }
      }
    })
  }

  def namedThreadFactory(prefix: String): ThreadFactory = {
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat(s"${prefix}-%d").build()
  }

  def newDaemonCachedThreadPool(prefix: String, maxThreads: Int, keepAliveSecond: Int = 60): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    val threadPool = new ThreadPoolExecutor(
      maxThreads,
      maxThreads,
      keepAliveSecond,
      TimeUnit.SECONDS,
      new LinkedBlockingDeque[Runnable],
      threadFactory
    )
    threadPool.allowCoreThreadTimeOut(true)
    threadPool
  }

  def newDaemonCachedThreadPool(prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonFixedThreadPool(nThreads: Int, prefix: String): ThreadPoolExecutor = {
    val threadFactory = namedThreadFactory(prefix)
    Executors.newFixedThreadPool(nThreads, threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def newDaemonSingleThreadExecutor(threadName: String): ExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    Executors.newSingleThreadExecutor(threadFactory)
  }

  def newDaemonSingleThreadScheduledExecutor(threadName: String): ScheduledExecutorService = {
    val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build()
    val executor = new ScheduledThreadPoolExecutor(1, threadFactory)
    executor.setRemoveOnCancelPolicy(true)
    executor
  }

}