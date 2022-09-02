package xyz.sourcecodestudy.rpc

import scala.concurrent.{Future}
import scala.concurrent.duration.{FiniteDuration}
import scala.util.control.NonFatal
import scala.concurrent.duration._

import java.util.concurrent.TimeoutException

import xyz.sourcecodestudy.rpc.{RpcSettings}

class RpcException(message: String, t: Throwable) extends Exception(message, t) {
  def this(message: String) = this(message, null)
}

class RpcTimeoutException(message: String, cause: TimeoutException) extends TimeoutException(message) {
  initCause(cause)
}

class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String) extends Serializable {

  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage + ". This timeout is controlled by " + timeoutProp, te)
  }

  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    case rte: RpcTimeoutException => throw rte
    case te: TimeoutException => throw createRpcTimeoutException(te)
    case e  => throw e
  }

  def awaitResult[T](future: Future[T]): T = {
    try {
      // CanAwait没有用到，类型转换占个位置
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      future.result(duration)(awaitPermission)
    } catch {
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new RpcException("Exception thrown in awaitResult: ", t)
    }
  }
}

object RpcTimeout {
  
  def apply(settings: RpcSettings, timeoutProps: String, defaultValue: String): RpcTimeout = {
    val timeout = settings.get("rpc.awaitTimeout", defaultValue).takeWhile(c => c.isDigit).toLong.seconds
    new RpcTimeout(timeout, timeoutProps)
  }
}
