package xyz.sourcecodestudy.rpc.util

import java.io.IOException

import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.{RpcSettings, RpcTimeout}

object Utils extends Logging {

  def getContextOrClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)

  def askRpcTimeout(settings: RpcSettings): RpcTimeout = {
    RpcTimeout(settings, "rpcTimeout", "120s")
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        throw e
      case NonFatal(e) =>
        throw e
    }
  }

  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(e) =>
        logger.error(s"Uncaught exeception in thread ${Thread.currentThread().getName}", e)
    }
  }
}