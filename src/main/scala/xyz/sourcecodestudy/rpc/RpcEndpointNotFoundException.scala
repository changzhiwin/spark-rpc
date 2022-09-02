package xyz.sourcecodestudy.rpc

class RpcEndpointNotFoundException(uri: String) extends RpcException(s"Cannot find endpoint: $uri")