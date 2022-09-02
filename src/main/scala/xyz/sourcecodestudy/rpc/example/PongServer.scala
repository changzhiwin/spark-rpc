package xyz.sourcecodestudy.rpc.demo

import org.apache.logging.log4j.scala.Logging

import xyz.sourcecodestudy.rpc.{RpcEnv, RpcSettings}

object PongServer extends Logging {

  def main(args: Array[String]): Unit = {
    
    val settings = new RpcSettings()
    val rpcEnv = RpcEnv.create("PingPongEnv", "127.0.0.1", 9992, 1, settings)

    val endpoint = new PingPongEndpoint(rpcEnv)

    // register endpoint, for process remote messages.
    rpcEnv.setupEndpoint("ping-pong-endpoint", endpoint)

    logger.info("Pong Server running...")

    rpcEnv.awaitTermination()
  }
}