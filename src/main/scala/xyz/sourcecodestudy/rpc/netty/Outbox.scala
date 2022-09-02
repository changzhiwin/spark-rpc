package xyz.sourcecodestudy.rpc.netty

import java.nio.ByteBuffer
import javax.annotation.concurrent.GuardedBy
import java.util.concurrent.Callable

import scala.util.control.NonFatal

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.network.client.{RpcResponseCallback, TransportClient}

import xyz.sourcecodestudy.rpc.{RpcAddress, RpcEnvStoppedException, RpcException}
import xyz.sourcecodestudy.rpc.netty.NettyRpcEnv

sealed trait OutboxMessage {
  def sendWith(client: TransportClient): Unit

  def onFailure(e: Throwable): Unit
}

case class OneWayOutboxMessage(content: ByteBuffer) extends OutboxMessage with Logging{

  override def sendWith(client: TransportClient): Unit = {
    client.send(content)
  }

  override def onFailure(e: Throwable): Unit = {
    e match {
      case es: RpcEnvStoppedException => logger.debug(es.getMessage)
      case _  => logger.warn(s"Failed to send one-way RPC", e)
    }
  }
}

case class RpcOutboxMessage(
    content: ByteBuffer,
    _onFailure: (Throwable) => Unit,
    _onSuccess: (TransportClient, ByteBuffer) => Unit) extends OutboxMessage with RpcResponseCallback with Logging  {

  private var client: Option[TransportClient] = None
  private var requestId: Option[Long] = None

  override def sendWith(client: TransportClient): Unit = {
    this.client = Some(client)
    this.requestId = Some(client.sendRpc(content, this))
  }

  override def onFailure(e: Throwable): Unit = {
    _onFailure(e)
  }

  def removeRpcRequest(): Unit = {
    client match {
      case Some(ct) => {
        requestId match {
          case Some(id) => ct.removeRpcRequest(id)
          case None     => logger.error("remove Rpc request failed, None requestId")
        }
      }
      case None     => 
        logger.error("remove Rpc request failed, None client")
    }
  }

  def onTimeout(): Unit = {
    removeRpcRequest()
  }

  def onAbort(): Unit = {
    removeRpcRequest()
  }

  def onSuccess(response: ByteBuffer): Unit = {
    client match {
      case Some(ct) => _onSuccess(ct, response)
      case None     => logger.error("onSuccess, None client")
    }
  }
}


class Outbox(nettyEnv: NettyRpcEnv, val address: RpcAddress) {
  
  outbox =>

  @GuardedBy("this")
  private val messages = new java.util.LinkedList[OutboxMessage]

  @GuardedBy("this")
  private var client: Option[TransportClient] = None

  @GuardedBy("this")
  private var connectFuture: Option[java.util.concurrent.Future[Unit]] = None

  @GuardedBy("this")
  private var stopped = false

  @GuardedBy("this")
  private var draining = false

  def send(message: OutboxMessage): Unit = {
    val dropped = synchronized {
      stopped match {
        case true  => true
        case false => {
          messages.add(message)
          false
        }
      }
    }

    dropped match {
      case true  => {
        message.onFailure(new RpcException("Message is dropped because Outbox is stoped"))
      }
      case false => {
        drainOutbox()
      }
    }
  }

  def drainOutbox(): Unit = {

    var message: Option[OutboxMessage] = None
    synchronized {
      // not ready
      if (stopped || connectFuture != None) return

      if (client == None) {
        launchConnectTask()
        return
      }

      if (draining) {
        // someone doing
        return
      }

      message = Option(messages.poll())
      message match {
        case None    => return     // Nothing to do
        case Some(_) => draining = true
      }
    }

    while (true) {
      try {
        client match {
          case Some(ct) => {
            message.get.sendWith(ct)
          }
          case None     => {
            assert(stopped)
          }
        }
      } catch {
        case NonFatal(e) => {
          handleNetworkFailure(e)
          return
        }
      } // end try

      synchronized {
        if (stopped) return

        message = Option(messages.poll())
        if (message == None) {
          draining = false
          return
        }
      }
    } // end while
  } // def drainOutbox()

  def launchConnectTask(): Unit = {
    connectFuture = Option(nettyEnv.clientConnectionExecutor.submit(new Callable[Unit] {

      override def call(): Unit = {
        try {
          val _client = nettyEnv.createClient(address)
          outbox.synchronized {
            client = Some(_client)

            if (stopped) closeClient()
          }
        } catch {
          case ie: InterruptedException => return
          case NonFatal(e)                        => {
            outbox.synchronized { connectFuture = None }
            handleNetworkFailure(e)
            return
          }
        }

        outbox.synchronized { connectFuture = None }

        drainOutbox()
      }
    }))
  } // end launchConnectTask()

  def handleNetworkFailure(e: Throwable): Unit = {
    synchronized {
      assert(connectFuture == None)

      if (stopped) return

      stopped = true

      closeClient()
    }

    nettyEnv.removeOutbox(address)

    cleanMessageList(e)
  }

  def closeClient(): Unit = synchronized {
    client = None
  }

  def stop(): Unit = {
    synchronized {
      if (stopped) return

      stopped = true
      if (connectFuture != None) connectFuture.get.cancel(true)
      closeClient()
    }

    cleanMessageList(new RpcException("Message is dropped, Outbox is stopped."))
  }

  private def cleanMessageList(e: Throwable): Unit = {
    var message = Option(messages.poll())
    while (message != None) {
      message.get.onFailure(e)
      message = Option(messages.poll())
    }
    assert(messages.isEmpty)  
  }
}