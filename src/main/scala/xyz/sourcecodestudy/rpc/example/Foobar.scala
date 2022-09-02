package xyz.sourcecodestudy.rpc.demo

import scala.util.{Success, Failure}

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.RpcSettings
import xyz.sourcecodestudy.rpc.util.ThreadUtils
import xyz.sourcecodestudy.rpc.{ThreadSafeRpcEndpoint, IsolatedRpcEndpoint, RpcEnv, RpcCallContext}

sealed trait SomeInfo

case class Tell(secret: String) extends SomeInfo

case class Ask(question: String) extends SomeInfo

class FoobarSafeEndpoint(override val rpcEnv: RpcEnv, val name: String) extends ThreadSafeRpcEndpoint with Logging {

  val qa = Map[String, String]("1" -> "apple", "2" -> "banana", "3" -> "orange")

  override def onStart(): Unit = {
    logger.info(s"[${name}] start.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Tell(secret) => logger.info(s"[${name}] receive secret? ${secret}")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Ask(question) => {
      logger.info(s"[${name}] receive question: ${question}")
      context.reply(qa.getOrElse(question, "empty"))
    }
  }
}

class FoobarIsoEndpoint(override val rpcEnv: RpcEnv, val name: String) extends IsolatedRpcEndpoint with Logging {

  val qa = Map[String, String]("1" -> "Cat", "2" -> "Dog", "3" -> "Fish")

  override def onStart(): Unit = {
    logger.info(s"[${name}] start.")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case Tell(secret) => logger.info(s"[${name}] receive secret? ${secret}")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case Ask(question) => {
      logger.info(s"[${name}] receive question: ${question}")
      context.reply(qa.getOrElse(question, "empty"))
    }
  }
}

class Foobar(rpcEnv: RpcEnv, isIso: Boolean) extends Logging {

  val foobarEndpoint = isIso match {
    case false => new FoobarSafeEndpoint(rpcEnv, "ThreadSafe")
    case true  => new FoobarIsoEndpoint(rpcEnv, "Isolate")
  }

  val endpointRef = rpcEnv.setupEndpoint("foobarTest", foobarEndpoint)

  def tellSome(secret: String): Unit = {
    logger.info("tellSome()")
    endpointRef.send(Tell(secret))
  }

  def askSome(question: String): Unit = {
    logger.info("askSome()")
    endpointRef.ask[String](Ask(question)).onComplete {
      case Success(answer) => logger.info(s"Get answer: ${answer}")
      case Failure(e)      => logger.error(s"Get Exception: ${e}")
    }(ThreadUtils.sameThread)
  }

  def askAndWait(question: String): Unit = {
    logger.info("askAndWait()")
    val ans = endpointRef.askSync[String](Ask(question))
    logger.info(s"Wait answer: ${ans}")
  }
}

object Foobar {
  
  def main(args: Array[String]) = {

    val settings = new RpcSettings()

    val rpcEnv = RpcEnv.create("FoobarEnv", "127.0.0.1", 9999, 1, settings)

    val foobar = new Foobar(rpcEnv, false)

    foobar.tellSome("hi rpc")

    foobar.askSome("1")

    foobar.askAndWait("3")

    foobar.askSome("8")

    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
  }
}