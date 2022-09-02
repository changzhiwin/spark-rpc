package xyz.sourcecodestudy.rpc.demo

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.{ThreadSafeRpcEndpoint, RpcEnv, RpcCallContext}

sealed trait Info

case class Notify(secret: String) extends Info

case class Ping(question: Int) extends Info

case class Pong(answer: String) extends Info

class PingPongEndpoint(override val rpcEnv: RpcEnv) extends ThreadSafeRpcEndpoint with Logging {
  
  val name: String = "pingpong-endpoint"

  val qa = Map[Int, String](1 -> "apple", 2 -> "banana", 3 -> "orange")

  override def onStart(): Unit = {
    logger.info(s"[${name}] stared")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Notify(secret) => logger.warn(s"[${name}] receive secret: ${secret}")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Ping(question) => {
      logger.warn(s"[${name}] receive question: ${question}")
      context.reply(Pong(qa.getOrElse(question, "empty")))
    }
  }
}