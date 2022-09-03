package xyz.sourcecodestudy.rpc.example

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.RpcSettings
import xyz.sourcecodestudy.rpc.{RpcEnv, RpcEndpoint, RpcEndpointRef}

case class Greet(whom: String, replyTo: RpcEndpointRef)
case class Greeted(whom: String, from: RpcEndpointRef)

class HelloWorld(override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {

  override def receive: PartialFunction[Any, Unit] = {
    case Greet(whom, replyTo) => {

      logger.info(s"Hello ${whom}!")

      replyTo.send(Greeted(whom, self))
    }
  }

}

class HelloWorldBot(override val rpcEnv: RpcEnv, max: Int) extends RpcEndpoint with Logging {

  private var replyTimes = 0

  override def receive: PartialFunction[Any, Unit] = {
    case Greeted(whom, from) => {

      replyTimes += 1
      logger.info(s"Greeting ${replyTimes} for ${whom}")

      if (replyTimes < max) {
        from.send(Greet(whom, self))
      } else {
        rpcEnv.shutdown()
      }
    }
  }
}

object HelloWorldMain {

  def apply(message: String): Unit = {
    
    val settings = new RpcSettings()
    val rpcEnv = RpcEnv.create("hello", "127.0.0.1", 9999, 1, settings)

    val greeter = rpcEnv.setupEndpoint("greeter", new HelloWorld(rpcEnv))
    val replyTo = rpcEnv.setupEndpoint("replyer", new HelloWorldBot(rpcEnv, max = 3))

    greeter.send( Greet(message, replyTo) )

    rpcEnv.awaitTermination()
  }

  def main(args: Array[String]): Unit = {

    val message = args.toSeq match {
      case head :: tail => head
      case Nil    => "World"
    }

    HelloWorldMain(message)
  }
}