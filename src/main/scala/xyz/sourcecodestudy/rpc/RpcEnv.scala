package xyz.sourcecodestudy.rpc

import scala.concurrent.Future

import xyz.sourcecodestudy.rpc.RpcSettings
import xyz.sourcecodestudy.rpc.netty.{NettyRpcEnvFactory}

case class RpcEnvConfig(
    settings: RpcSettings,
    name: String,
    bindAddress: String,
    bindPort: Int,
    numUsableCores: Int)

object RpcEnv {
  def create(
      name: String,
      bindAddress: String,
      bindPort: Int,
      numUsableCores: Int,
      settings: RpcSettings): RpcEnv = {

    val config = RpcEnvConfig(settings, name, bindAddress, bindPort, numUsableCores)
    new NettyRpcEnvFactory().create(config)
  }
}

abstract class RpcEnv(settings: RpcSettings) {

  def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  def address: RpcAddress

  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  def stop(endpint: RpcEndpointRef): Unit

  def shutdown(): Unit

  def awaitTermination(): Unit

  def deserialize[T](deserAction: () => T): T
}

