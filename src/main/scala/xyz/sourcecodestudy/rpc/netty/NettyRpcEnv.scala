package xyz.sourcecodestudy.rpc.netty

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}
import java.nio.ByteBuffer
import java.io.{OutputStream, DataOutputStream, ByteArrayOutputStream}
import java.io.{DataInputStream, ObjectInputStream, ObjectOutputStream}

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Success, Failure}
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.network.client.{TransportClient, TransportClientBootstrap}
import org.apache.spark.network.server.{TransportServer, TransportServerBootstrap}
import org.apache.spark.network.util.{TransportConf, ConfigProvider}
import org.apache.spark.network.{TransportContext}

import xyz.sourcecodestudy.rpc.util.{ThreadUtils, ByteBufferInputStream}
import xyz.sourcecodestudy.rpc.serializer.{JavaSerializer, JavaSerializerInstance, SerializationStream}
import xyz.sourcecodestudy.rpc.{RpcEndpointRef, RpcEndpoint, RpcTimeout, RpcEnvConfig, RpcEnvFactory, RpcException, RpcSettings}
import xyz.sourcecodestudy.rpc.{RpcEnv, AbortableRpcFuture, RpcAddress, RpcEndpointAddress, RpcEnvStoppedException, RpcEndpointNotFoundException}

class NettyRpcEnv(
    val settings: RpcSettings, 
    javaSerializerInstance: JavaSerializerInstance,
    host: String,
    numUsableCores: Int) extends RpcEnv(settings) with Logging {
  
  val role = "executor"

  private var server: Option[TransportServer] = None

  private val stopped = new AtomicBoolean(false)

  override lazy val address: RpcAddress = {
    server match {
      case Some(ts) => RpcAddress(host, ts.getPort())
      case None    => throw new RpcException("Get lazy address failed.")
    }
  }

  private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)

  override def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.registerRpcEndpoint(name, endpoint)
  }

  override def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef] = {
    val addr = RpcEndpointAddress(uri)
    val endpointRef = new NettyRpcEndpointRef(settings, addr, this)
    val verifier = new NettyRpcEndpointRef(
      settings,
      RpcEndpointAddress(addr.rpcAddress, RpcEndpointVerifier.NAME),
      this)

    verifier.ask[Boolean](RpcEndpointVerifier.CheckExistence(endpointRef.name)).flatMap { find =>
      if (find) {
        Future.successful(endpointRef)
      } else {
        Future.failed(new RpcEndpointNotFoundException(uri))
      }
    }(ThreadUtils.sameThread)
  }

  override def stop(endpointRef: RpcEndpointRef): Unit = {
    require(endpointRef.isInstanceOf[NettyRpcEndpointRef])
    dispatcher.stop(endpointRef)
  }

  private val outboxes = new ConcurrentHashMap[RpcAddress, Outbox]()

  def removeOutbox(address: RpcAddress): Unit = {
    val outbox = Option(outboxes.remove(address))
    if (outbox != None) {
      outbox.get.stop()
    }
  }

  def send(message: RequestMessage): Unit = {
    message.receiver.address match {
      case Some(addr) => {

        val isLocal = (addr == address)
        logger.debug(s"[${message.senderAddress}] -> [${addr}], send [${if (isLocal) "local" else "remote"}] message [${message.content}]")
        
        if (isLocal) {
          try {
            dispatcher.postOneWayMessage(message)
          } catch {
            case e: RpcEnvStoppedException => logger.debug(e.getMessage)
          }
        } else {
          // 需要网络传输的情况，数据都序列化了
          postToOutbox(message.receiver, OneWayOutboxMessage(message.serialize(this)))
        }
      }
      case None => logger.error(s"RequestMessage remote address is None")
    }
  }

  def askAbortable[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    val promise = Promise[Any]()
    var rpcMsg: Option[RpcOutboxMessage] = None

    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        e match {
          case re: RpcEnvStoppedException =>
            logger.debug(s"askAbortable -> onFailure_stop, ${re}")
          case _ =>
            logger.warn(s"askAbortable -> onFailure_, ${e}")
        }
      }
    }

    def onSuccess(reply: Any): Unit = reply match {
      case RpcFailure(e) => onFailure(e)
      case rpcReply      => 
        if (!promise.trySuccess(rpcReply)) {
          logger.warn(s"Ignored message: ${rpcReply}")
        }
    }

    def onAbort(t: Throwable): Unit = {
      onFailure(t)
      rpcMsg.foreach { _.onAbort() }
    }

    try {
      message.receiver.address match {
        case Some(addr) => {

          val isLocal = (addr == address)
          logger.debug(s"[${message.senderAddress}] -> [${addr}], ask [${if (isLocal) "local" else "remote"}] message [${message.content}]")

          if (isLocal) {
            val p = Promise[Any]()
            p.future.onComplete {
              case Success(response) => onSuccess(response)
              case Failure(e) => onFailure(e)
            }(ThreadUtils.sameThread)
        
            dispatcher.postLocalMessage(message, p)
          } else {
            // 发送给远端
            val rpcMessage = RpcOutboxMessage(
              message.serialize(this),
              onFailure,
              (client, response) => onSuccess(deserialize[Any](client, response)) 
            )

            postToOutbox(message.receiver, rpcMessage)
            rpcMsg = Option(rpcMessage)

            // 这是为了处理future失败？还需要探究一下TODO
            promise.future.failed.foreach {
              case _: TimeoutException => rpcMessage.onTimeout()
              case _ =>
            }(ThreadUtils.sameThread)
          }
        }
        case None => logger.error(s"RequestMessage remote address is None")
      }

      // 暂未实现超时逻辑，TODO
    } catch {
      case NonFatal(e) =>
        onFailure(e)
    }

    new AbortableRpcFuture[T](
      promise.future.mapTo[T].recover(timeout.addMessageIfTimeout)(ThreadUtils.sameThread),
      onAbort
    )
  }

  def ask[T: ClassTag](message: RequestMessage, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  override def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef = {
    dispatcher.getRpcEndpointRef(endpoint)
  }

  def serializeStream(out: OutputStream): SerializationStream = {
    javaSerializerInstance.serializeStream(out)
  }

  def serialize(content: Any): ByteBuffer = {
    javaSerializerInstance.serialize(content)
  }

  override def deserialize[T](deserAction: () => T): T = {
    NettyRpcEnv.currentEnv.withValue(this) {
      deserAction()
    }
  }

  def deserialize[T: ClassTag](client: TransportClient, bytes: ByteBuffer): T = {
    NettyRpcEnv.currentClient.withValue(client) {
      deserialize { 
        () => javaSerializerInstance.deserialize[T](bytes)
      }
    }
  }

  override def awaitTermination(): Unit = {
    dispatcher.awaitTermination()
  }

  override def shutdown(): Unit = {
    if (!stopped.compareAndSet(false, true)) {
      return
    }

    val iter = outboxes.values().iterator
    while (iter.hasNext()) {
      val outbox = iter.next()
      outboxes.remove(outbox.address)
      outbox.stop()
    }

    if (dispatcher != null) {
      dispatcher.stop()
    }

    if (server != None) {
      server.get.close()
    }

    if (clientFactory != null) {
      clientFactory.close()
    }

    if (clientConnectionExecutor != null) {
      clientConnectionExecutor.shutdownNow()
    }

    if (transportContext != null) {
      transportContext.close()
    }
  }

  private def postToOutbox(receiver: NettyRpcEndpointRef, message: OutboxMessage): Unit = {
    // 什么场景会直接send，需要探究
    if (receiver.client != null) {
      message.sendWith(receiver.client)
    } else {
      require(receiver.address != None, "Cannot send message to client endpoint with no listen address")

      val receiveAddr = receiver.address.get
      val targetOutbox = {
        Option(outboxes.get(receiveAddr)) match {
          case Some(outbox) => outbox
          case None         => {
            val newOutbox = new Outbox(this, receiveAddr)
            Option(outboxes.putIfAbsent(receiveAddr, newOutbox)) match {
              case None      => newOutbox
              case Some(oldOudbox) => oldOudbox
            }
          } 
        }
      }// end val targetOutbox

      if (stopped.get) {
        outboxes.remove(receiveAddr)
        targetOutbox.stop()
      } else {
        targetOutbox.send(message)
      }
    }
  }

  // 以下都是处理网络逻辑

  private def getConfigFromRpcSettings: ConfigProvider = {
    val config = settings
    new ConfigProvider {
      override def get(name: String): String = config.get(name)
      override def get(name: String, defaultValue: String): String = config.get(name, defaultValue)
      override def getAll(): java.lang.Iterable[java.util.Map.Entry[String, String]] = {
        //Seq[(String, String)]().toMap.asJava.entrySet()
        config.getAll.toMap.asJava.entrySet()
      }
    }
  }

  val transportConf = new TransportConf("rpc", getConfigFromRpcSettings)

  // Just mock, have not real action
  val streamManager = new NettyStreamManager()

  private val transportContext = new TransportContext(transportConf, 
    new NettyRpcHandler(dispatcher, this, streamManager))

  val clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connection", settings.get("rpc.connect.threads", "4").toInt)

  private def createClientBootstraps(): java.util.List[TransportClientBootstrap] = {
    java.util.Collections.emptyList[TransportClientBootstrap]
  }

  private val clientFactory = transportContext.createClientFactory(createClientBootstraps())

  def createClient(address: RpcAddress): TransportClient = {
    clientFactory.createClient(address.host, address.port)
  }

  def startServer(bindAddress: String, port: Int): Unit = {
    val bootstraps: java.util.List[TransportServerBootstrap] = java.util.Collections.emptyList[TransportServerBootstrap]
    server = Option(transportContext.createServer(bindAddress, port, bootstraps))
    // 实现了一个ask指令，通过name检查是否该name的endpoint是否注册
    dispatcher.registerRpcEndpoint(
      RpcEndpointVerifier.NAME,
      new RpcEndpointVerifier(this, dispatcher)
    )
  }
}

object NettyRpcEnv {
  
  // 反序列化时，初始化必要属性，如rpcEnv
  val currentEnv = new scala.util.DynamicVariable[NettyRpcEnv](null)

  val currentClient = new scala.util.DynamicVariable[TransportClient](null)
}

class NettyRpcEnvFactory extends RpcEnvFactory {
  def create(config: RpcEnvConfig): RpcEnv = {
    val settings = config.settings
    val javaSerializerInstance = new JavaSerializer().newInstance().asInstanceOf[JavaSerializerInstance]
    val nettyEnv = new NettyRpcEnv(settings, javaSerializerInstance, host = config.bindAddress, config.numUsableCores)

    try {
      nettyEnv.startServer(config.bindAddress, config.bindPort)
    } catch {
      case e : Throwable => {
        nettyEnv.shutdown()
        throw e
      }
    }
    nettyEnv
  }
}

case class RpcFailure(e: Throwable)

class NettyRpcEndpointRef(
    @transient private val settings: RpcSettings,
    var endpointAddress: RpcEndpointAddress,
    @transient private var nettyEnv: NettyRpcEnv) extends RpcEndpointRef(settings) {

  @transient var client: TransportClient = _

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    nettyEnv = NettyRpcEnv.currentEnv.value
    client = NettyRpcEnv.currentClient.value
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
  }

  override def address: Option[RpcAddress] = endpointAddress.rpcAddress

  override def name: String = endpointAddress.name

  override def askAbortable[T: ClassTag](message: Any, timeout: RpcTimeout): AbortableRpcFuture[T] = {
    nettyEnv.askAbortable(new RequestMessage(nettyEnv.address, this, message), timeout)
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    askAbortable(message, timeout).future
  }

  override def send(message: Any): Unit = {
    require(message != null, "Message is null")
    nettyEnv.send(new RequestMessage(nettyEnv.address, this, message))
  }

  override def toString: String = s"NettyRpcEndpointRef(${endpointAddress})"

  final override def equals(that: Any): Boolean = that match {
    case other: NettyRpcEndpointRef => endpointAddress == other.endpointAddress
    case _ => false
  }

  final override def hashCode(): Int = endpointAddress match {
    case null => 0
    case _    => endpointAddress.hashCode()
  }
}

class RequestMessage(
    val senderAddress: RpcAddress,
    val receiver: NettyRpcEndpointRef,
    val content: Any) {
  
  def serialize(nettyEnv: NettyRpcEnv): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = new DataOutputStream(bos)

    try {
      writeRpcAddress(out, Option(senderAddress))
      writeRpcAddress(out, receiver.address)
      out.writeUTF(receiver.name)

      val s = nettyEnv.serializeStream(out)
      try { s.writeObject(content) } finally { s.close() }

    } finally {
      out.close()
    }

    ByteBuffer.wrap(bos.toByteArray)
  }

  private def writeRpcAddress(out: DataOutputStream, rpcAddress: Option[RpcAddress]): Unit = {
    rpcAddress match {
      case None => out.writeBoolean(false)
      case Some(addr)    => {
        out.writeBoolean(true)
        out.writeUTF(addr.host)
        out.writeInt(addr.port)
      }
    }
  }

  override def toString: String = s"RequestMessage(${senderAddress}, ${receiver}, ${content})"
}

object RequestMessage extends Logging{
  
  private def readRpcAddress(in: DataInputStream): Option[RpcAddress] = {
    val hasRpcAddress = in.readBoolean()
    if (hasRpcAddress) {
      Some(RpcAddress(in.readUTF(), in.readInt()))
    } else {
      None
    }
  }

  def apply(nettyEnv: NettyRpcEnv, client: TransportClient, bytes: ByteBuffer): RequestMessage = {
    // Must use ByteBufferInputStream to wrapper, due to ref bytes
    val bis = new ByteBufferInputStream(bytes)
    val in = new DataInputStream(bis)

    try {
      val senderAddress = readRpcAddress(in) match {
        case Some(addr) => addr
        case None       => throw new IllegalStateException("senderAddress must be have")
      }
      val endpointAddress = RpcEndpointAddress(readRpcAddress(in), in.readUTF())

      val ref = new NettyRpcEndpointRef(nettyEnv.settings, endpointAddress, nettyEnv)
      ref.client = client
      new RequestMessage(
        senderAddress,
        ref, 
        nettyEnv.deserialize[Any](client, bytes) // 剩下的byte是消息体
      )
    } catch {
      case e: Throwable =>
        logger.error("nettyEnv.deserialize error", e)
        throw new RpcException(e.getMessage)
    } finally {
      in.close()
    }
  }
}