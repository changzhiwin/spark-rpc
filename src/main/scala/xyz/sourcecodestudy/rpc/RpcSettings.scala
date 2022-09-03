package xyz.sourcecodestudy.rpc

import scala.collection.mutable.HashMap
import scala.jdk.CollectionConverters._

import com.typesafe.config.{ConfigFactory, Config}

class RpcSettings(val config: Config) extends Cloneable {

  def this() = {
      this(ConfigFactory.load())
  }

  private val setting = new HashMap[String, String]()

  config.entrySet().asScala.foreach { item =>
    val key = item.getKey()
    if (key.startsWith("rpc.")) {
      setting(key) = config.getString(key)
      // println(key + " -> " + setting(key))
    }
  }

  def set(key: String, value: String): RpcSettings = {
    (key, value) match {
      case (k: String, v: String) => setting(k) = v
      case _ => new NullPointerException("key / value is Null")
    }
    this
  }

  def setAll(settings: Iterable[(String, String)]) = {
    this.setting ++= settings
    this
  }

  def getOption(key: String): Option[String] = setting.get(key)

  def get(key: String): String =
    setting.getOrElse(key, throw new NoSuchElementException(key))

  def get(key: String, defaultValue: String): String =
    setting.getOrElse(key, defaultValue)

  def getAll: Array[(String, String)] = {
    setting.toArray
  }

  def contains(key: String): Boolean = setting.contains(key)

  override def clone: RpcSettings = {
    new RpcSettings(config).setAll(setting)
  }

  def toDebugString: String = {
    setting.toArray.sorted.map{case (k, v) => s"$k=$v"}.mkString("\n")
  }
}