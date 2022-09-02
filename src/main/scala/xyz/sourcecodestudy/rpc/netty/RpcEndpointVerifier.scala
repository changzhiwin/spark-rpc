package xyz.sourcecodestudy.rpc.netty

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

class RpcEndpointVerifier(override val rpcEnv: RpcEnv, dispatcher: Dispatcher) extends RpcEndpoint with Logging {

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RpcEndpointVerifier.CheckExistence(name) => context.reply(dispatcher.verify(name))
  }

  override def onStart(): Unit = {
    logger.info(s"[${RpcEndpointVerifier.NAME}] stared")
  }
}

object RpcEndpointVerifier {
  val NAME = "endpoint-verifier"

  case class CheckExistence(name: String)
}