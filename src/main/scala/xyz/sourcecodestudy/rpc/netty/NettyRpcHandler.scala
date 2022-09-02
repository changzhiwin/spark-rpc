package xyz.sourcecodestudy.rpc.netty

import java.nio.ByteBuffer
import java.net.{InetSocketAddress}
import java.util.concurrent.ConcurrentHashMap

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}
import org.apache.spark.network.server.{RpcHandler, StreamManager}

import xyz.sourcecodestudy.rpc.{RpcAddress, RpcException}

class NettyRpcHandler(
    dispatcher: Dispatcher,
    nettyEnv: NettyRpcEnv,
    streamManager: StreamManager) extends RpcHandler with Logging {

  private val remoteAddresses = new ConcurrentHashMap[RpcAddress, RpcAddress]()

  override def receive(
      client: TransportClient,
      message: ByteBuffer,
      callback: RpcResponseCallback): Unit = {

    //logger.info("receive request and callback")
    
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postRemoteMessage(messageToDispatch, callback)
  }
  
  override def receive(
      client: TransportClient,
      message: ByteBuffer): Unit = {
      
    //logger.info("receive request only")
    
    val messageToDispatch = internalReceive(client, message)
    dispatcher.postOneWayMessage(messageToDispatch)
  }

  override def getStreamManager: StreamManager = streamManager

  override def exceptionCaught(cause: Throwable, client: TransportClient): Unit = {
    
    val address = Option(client.getSocketAddress().asInstanceOf[InetSocketAddress])
    address match {
        case Some(addr) => {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          dispatcher.postToAll(RemoteProcessConnectionError(cause, clientAddr))

          Option(remoteAddresses.get(clientAddr)).foreach { remoteEnvAddress =>
            dispatcher.postToAll(RemoteProcessConnectionError(cause, remoteEnvAddress))
          }
        }
        case None       => {
            logger.error("Exception before connecting to the client", cause)
        }
    }
  }

  override def channelActive(client: TransportClient): Unit = {
     val address = Option(client.getSocketAddress().asInstanceOf[InetSocketAddress])
     address match {
        case Some(addr) => {
          val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
          dispatcher.postToAll(RemoteProcessConnected(clientAddr))
        }
        case None        => {
          throw new RpcException("No address after channel active.")
        }
     }
  }

  override def channelInactive(client: TransportClient): Unit = {

     Option(client.getSocketAddress().asInstanceOf[InetSocketAddress]).foreach { addr =>
        val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
        nettyEnv.removeOutbox(clientAddr)
        dispatcher.postToAll(RemoteProcessDisconnected(clientAddr))

        Option(remoteAddresses.get(clientAddr)).foreach { remoteEnvAddress =>
          dispatcher.postToAll(RemoteProcessDisconnected(remoteEnvAddress))
        }
    }
  }

  private def internalReceive(client: TransportClient, message: ByteBuffer): RequestMessage  = {
    val addr = client.getSocketAddress().asInstanceOf[InetSocketAddress]
    assert(addr != null)

    val clientAddr = RpcAddress(addr.getHostString, addr.getPort)
    //logger.info(s"internalReceive client address ${clientAddr}")

    val requestMessage = RequestMessage(nettyEnv, client, message)
    //logger.info(s"internalReceive remoteEnvAddress ${requestMessage.senderAddress}")

    val remoteEnvAddress = requestMessage.senderAddress
    // clientAddr 表示是本次socket连接中远程的ip+port
    // remoteEnvAddress 表示是对方NettyEnv监听的ip+port
    if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
        dispatcher.postToAll(RemoteProcessConnected(remoteEnvAddress))
    }

    requestMessage
  }
}