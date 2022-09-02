package xyz.sourcecodestudy.rpc.serializer

import java.nio.ByteBuffer
import java.io.{OutputStream, InputStream, ObjectStreamClass}
import java.io.{ObjectOutputStream, ObjectInputStream}
import java.io.{ByteArrayOutputStream}

import scala.reflect.ClassTag

import xyz.sourcecodestudy.rpc.util.{Utils, ByteBufferInputStream}

class JavaSerializer() extends Serializer {

  def newInstance(): SerializerInstance = new JavaSerializerInstance()
}

class JavaSerializerInstance() extends SerializerInstance {

  def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteArrayOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    ByteBuffer.wrap(bos.toByteArray)
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject().asInstanceOf[T]
  }

  def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis, loader)
    in.readObject().asInstanceOf[T]
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, Utils.getContextOrClassLoader)
  }

  def deserializeStream(s: InputStream, loader: ClassLoader): DeserializationStream = {
    new JavaDeserializationStream(s, loader)
  }

}

class JavaSerializationStream(out: OutputStream) extends SerializationStream {

  private val objOut = new ObjectOutputStream(out)

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    objOut.writeObject(t)
    this
  }

  def flush(): Unit = objOut.flush()

  def close(): Unit = objOut.close()

}

class JavaDeserializationStream(in: InputStream, loader: ClassLoader) extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {
    override def resolveClass(desc: ObjectStreamClass) = 
      Class.forName(desc.getName, false, loader)
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]

  def close(): Unit = objIn.close()

}