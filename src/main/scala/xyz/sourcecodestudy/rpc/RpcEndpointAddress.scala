package xyz.sourcecodestudy.rpc

case class RpcEndpointAddress(rpcAddress: Option[RpcAddress], name: String) {
  require(name != null, "RpcEndpoint name must be provided")

  def this(host: String, port: Int, name: String) = {
    this(Some(RpcAddress(host, port)), name)
  }

  override def toString(): String = rpcAddress match {
    case Some(addr) => s"rpc://${name}@${addr.host}:${addr.port}"
    case None    => s"rpc-client://${name}"
  }
}

object RpcEndpointAddress {

  def apply(host: String, port:Int, name: String): RpcEndpointAddress = {
    new RpcEndpointAddress(host, port, name)
  }

  def apply(rpcUrl: String): RpcEndpointAddress = {
    try {
      val uri = new java.net.URI(rpcUrl)
      val host = uri.getHost()
      val port = uri.getPort()
      val name = uri.getUserInfo()
      if (uri.getScheme() != "rpc" || host == null || port < 0 || name == null) {
        throw new RpcException(s"Invalid Rpc URL: ${rpcUrl}")
      }
      new RpcEndpointAddress(host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new RpcException(s"Invalid Rpc URL: ${rpcUrl}", e)
    }
  }
}