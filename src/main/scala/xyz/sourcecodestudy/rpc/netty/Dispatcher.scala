package xyz.sourcecodestudy.rpc.netty

import javax.annotation.concurrent.GuardedBy
import java.util.concurrent.{ConcurrentMap, ConcurrentHashMap, CountDownLatch}

import scala.jdk.CollectionConverters._
import scala.concurrent.Promise
import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.network.client.{RpcResponseCallback}

import xyz.sourcecodestudy.rpc.{RpcEndpoint, IsolatedRpcEndpoint, RpcEndpointRef, RpcEndpointAddress, RpcEnvStoppedException, RpcException}

class Dispatcher(nettyEnv: NettyRpcEnv, numUsableCores: Int) extends Logging {
  
  private val endpoints: ConcurrentMap[String, MessageLoop] = new ConcurrentHashMap[String, MessageLoop]

  private val endpointRefs: ConcurrentMap[RpcEndpoint, RpcEndpointRef] = new ConcurrentHashMap[RpcEndpoint, RpcEndpointRef]

  private val shutdownLatch = new CountDownLatch(1)

  private lazy val sharedLoop = new SharedMessageLoop(nettyEnv.settings, this, numUsableCores)

  @GuardedBy("this")
  private var stopped = false

  def registerRpcEndpoint(name: String, endpoint: RpcEndpoint): NettyRpcEndpointRef = {
    val addr = RpcEndpointAddress(Some(nettyEnv.address), name)
    val endpointRef = new NettyRpcEndpointRef(nettyEnv.settings, addr, nettyEnv)

    synchronized {
      if (stopped) {
        throw new IllegalStateException("RpcEnv has been stopped")
      }

      if (endpoints.containsKey(name)) {
        throw new IllegalArgumentException(s"Rpcendpoint [${name}} had exists]]")
      }

      endpointRefs.put(endpoint, endpointRef)

      var messageLoop: MessageLoop = null
      try {
        messageLoop = endpoint match {
          case iso: IsolatedRpcEndpoint =>
            new DedicatedMessageLoop(name, iso, this)
          case _ =>
            sharedLoop.register(name, endpoint)
            sharedLoop 
        }
        endpoints.put(name, messageLoop)
      } catch {
        case NonFatal(e) =>
          endpointRefs.remove(endpoint)
          throw e
      }
    }
    endpointRef
  }

  private def unregisterRpcEndpoint(name: String): Unit = {
    val loop = endpoints.remove(name)
    if (loop != null) {
      loop.unregister(name)
    }
    // 注意：这里没有删除endpointRefs
  }

  def getRpcEndpointRef(endpoint: RpcEndpoint): RpcEndpointRef = endpointRefs.get(endpoint)

  def removeRpcEndpointRef(endpoint: RpcEndpoint): Unit = endpointRefs.remove(endpoint)

  def postToAll(message: InboxMessage): Unit = {
    val iter = endpoints.keySet().iterator()
    while (iter.hasNext) {
      val name = iter.next
      postMessage(name, message, {
        case e: RpcEnvStoppedException => logger.debug(s"Message ${message} dropped. ${e.getMessage}")
        case e: Throwable              => logger.warn(s"Message ${message} dropped. ${e.getMessage}")
      })
    }
  }

  def postRemoteMessage(message: RequestMessage, callback: RpcResponseCallback): Unit = {
    // 自身需要向外发送消息
    val rpcCallContext = new RemoteNettyRpcCallContext(nettyEnv, callback, message.senderAddress)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => callback.onFailure(e))
  }

  def postLocalMessage(message: RequestMessage, p: Promise[Any]): Unit = {
    // 自身向自身发送消息
    val rpcCallContext = new LocalNettyRpcCallContext(message.senderAddress, p)
    val rpcMessage = RpcMessage(message.senderAddress, message.content, rpcCallContext)
    postMessage(message.receiver.name, rpcMessage, (e) => p.tryFailure(e))
  }

  def postOneWayMessage(message: RequestMessage): Unit = {
    postMessage(message.receiver.name, OneWayMessage(message.senderAddress, message.content), {
      case re: RpcEnvStoppedException  => logger.debug(s"Message ${message} dropped. ${re.getMessage}")
      case e                           => throw e
    })
  }

  // 实际投递消息的接口
  private def postMessage(endpointName: String, message: InboxMessage, callbackIfStopped: (Exception) => Unit): Unit = {
    logger.debug(s"postMessage endpointName = ${endpointName}, message = ${message}")
    val error = synchronized {
      val loop = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (loop == null) {
        Some(new RpcException(s"Could not found ${endpointName}"))
      } else {
        loop.post(endpointName, message)
        None
      }
    }

    error.foreach(callbackIfStopped)
  }

  // 单个endpoint
  def stop(rpcEndpointRef: RpcEndpointRef): Unit = {
    synchronized {
      if (stopped) return

      unregisterRpcEndpoint(rpcEndpointRef.name)
    }
  }

  // 停止dispatcher
  def stop(): Unit = {
    synchronized {
      if (stopped) return
      stopped = true
    }

    var stopSharedLoop = false
    endpoints.asScala.foreach { case (name, loop) =>
      unregisterRpcEndpoint(name)
      if (!loop.isInstanceOf[SharedMessageLoop]) {
        loop.stop()
      } else {
        stopSharedLoop = true
      }
    }

    if (stopSharedLoop) sharedLoop.stop()

    shutdownLatch.countDown()
  }

  def awaitTermination(): Unit = {
    shutdownLatch.await()
  }

  def verify(name: String): Boolean = {
    endpoints.containsKey(name)
  }
}