package xyz.sourcecodestudy.rpc.netty

import scala.concurrent.Promise

import org.apache.spark.network.client.{RpcResponseCallback}

import xyz.sourcecodestudy.rpc.{RpcCallContext, RpcAddress}


abstract class NettyRpcCallContext(override val senderAddress: RpcAddress) extends RpcCallContext {

  protected def send(message: Any): Unit

  override def reply(response: Any): Unit = {
    send(response)
  }

  override def sendFailure(e: Throwable): Unit = {
    send(RpcFailure(e))
  }
}

class LocalNettyRpcCallContext(senderAddress: RpcAddress, p: Promise[Any]) extends NettyRpcCallContext(senderAddress) {
  override protected def send(message: Any): Unit = {
    p.success(message)
  }
}

class RemoteNettyRpcCallContext(
    nettyEnv: NettyRpcEnv,
    callback: RpcResponseCallback,
    senderAddress: RpcAddress)
  extends NettyRpcCallContext(senderAddress) {
  override protected def send(message: Any): Unit = {
    val reply = nettyEnv.serialize(message)
    callback.onSuccess(reply)
  }
}