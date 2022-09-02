package xyz.sourcecodestudy.rpc

case class RpcAddress(host: String, port: Int) {
  
  def hostPort: String = s"${host}:${port}"

  override def toString: String = hostPort
}

object RpcAddress {
  
  def fromURIString(uri: String): RpcAddress = {
    val uriObj = new java.net.URI(uri)
    RpcAddress(uriObj.getHost, uriObj.getPort)
  }
}