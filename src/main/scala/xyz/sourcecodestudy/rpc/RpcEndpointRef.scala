package xyz.sourcecodestudy.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import xyz.sourcecodestudy.rpc.RpcSettings
import xyz.sourcecodestudy.rpc.util.Utils

class RpcAbortException(message: String) extends Exception(message)

class AbortableRpcFuture[T: ClassTag](val future: Future[T], onAbort: Throwable => Unit) {
  def abort(t: Throwable): Unit = onAbort(t)
}

abstract class RpcEndpointRef(settings: RpcSettings) extends Serializable {
  
  val defaultAskTimeout = Utils.askRpcTimeout(settings)

  def address: Option[RpcAddress]

  def name: String

  def send(message: Any): Unit

  def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    throw new UnsupportedOperationException()
  }

  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }
}