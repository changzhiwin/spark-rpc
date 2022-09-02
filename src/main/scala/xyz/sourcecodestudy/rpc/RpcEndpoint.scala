package xyz.sourcecodestudy.rpc

trait RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv
}

trait RpcEndpoint {

  val rpcEnv: RpcEnv

  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new RpcException("Does not implement 'receive'")
  }

  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new RpcException("Won't reply anything"))
  }

  def onError(cause: Throwable): Unit = {
    throw cause
  }

  def onConnected(remoteAddress: RpcAddress): Unit = {
    // do nothing
  }

  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // do nothing
  }

  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // do nothing
  }

  def onStart(): Unit = {}

  def onStop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

trait ThreadSafeRpcEndpoint extends RpcEndpoint

// 该类型，会使用独立的线程处理消息；建议使用默认值1
trait IsolatedRpcEndpoint extends RpcEndpoint {
  def threadCount(): Int = 1
}